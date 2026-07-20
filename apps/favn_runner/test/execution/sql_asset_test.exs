defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Asset.RelationInput
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerWork
  alias Favn.Contracts.ResourceOutcome
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.Plan.NodeIdentity
  alias Favn.RelationRef
  alias Favn.Resource.Ref, as: ResourceRef
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQL.Check
  alias Favn.SQL.Column
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Param
  alias Favn.SQL.SessionRequirements
  alias Favn.SQL.Template

  setup do
    previous_test_pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid)
    previous_target_exists = Application.get_env(:favn_runner, :checked_target_exists)
    previous_cleanup_failure = Application.get_env(:favn_runner, :checked_cleanup_failure)
    previous_rollback_failure = Application.get_env(:favn_runner, :checked_rollback_failure)
    previous_begin_failure = Application.get_env(:favn_runner, :checked_begin_failure)
    previous_checked_columns = Application.get_env(:favn_runner, :checked_columns)

    previous_contract_outcomes =
      Application.get_env(:favn_runner, :checked_contract_outcomes)

    previous_runtime_inputs_resolved =
      Application.get_env(:favn_runner, :runtime_inputs_resolved)

    Application.put_env(:favn_runner, :execution_sql_asset_test_pid, self())
    Application.put_env(:favn_runner, :checked_target_exists, true)
    Application.put_env(:favn_runner, :checked_cleanup_failure, false)
    Application.put_env(:favn_runner, :checked_rollback_failure, false)
    Application.put_env(:favn_runner, :checked_begin_failure, false)
    Application.put_env(:favn_runner, :checked_contract_outcomes, [])

    Application.put_env(:favn_runner, :checked_columns, [
      %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: true}
    ])

    Application.put_env(:favn_runner, :runtime_inputs_resolved, false)

    on_exit(fn ->
      restore_env(:execution_sql_asset_test_pid, previous_test_pid)
      restore_env(:checked_target_exists, previous_target_exists)
      restore_env(:checked_cleanup_failure, previous_cleanup_failure)
      restore_env(:checked_rollback_failure, previous_rollback_failure)
      restore_env(:checked_begin_failure, previous_begin_failure)
      restore_env(:checked_columns, previous_checked_columns)
      restore_env(:checked_contract_outcomes, previous_contract_outcomes)
      restore_env(:runtime_inputs_resolved, previous_runtime_inputs_resolved)
      Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
    end)

    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeExecutionAdapter)

    :ok
  end

  test "manifest execution scopes SQL sessions to rendered target catalog" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}

    relation =
      RelationRef.new!(%{
        connection: :runner_sql_runtime,
        catalog: "raw",
        schema: "sales",
        name: "manifest_sql_asset"
      })

    version = register_sql_manifest!(ref, relation)

    work = %RunnerWork{
      run_id: "run_sql_catalog_scope",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok

    assert [
             %ResourceOutcome{
               resource: %ResourceRef{kind: :connection, name: "runner_sql_runtime"},
               status: :success
             }
           ] = result.resource_outcomes

    assert_received {:connect_opts, :runner_sql_runtime, opts}
    assert Keyword.fetch!(opts, :required_catalogs) == ["raw"]
  end

  test "manifest execution passes declared session resources to the SQL client" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}

    version =
      register_sql_manifest!(
        ref,
        nil,
        [],
        SessionRequirements.new!([:landing_storage, :azure_extension])
      )

    work = %RunnerWork{
      run_id: "run_sql_resource_scope",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }

    assert {:ok, %{status: :ok}} = FavnRunner.run(work)
    assert_received {:connect_opts, :runner_sql_runtime, opts}
    assert Keyword.fetch!(opts, :required_catalogs) == []
    assert Keyword.fetch!(opts, :required_resources) == ["azure_extension", "landing_storage"]
  end

  test "manifest execution scopes SQL sessions to declared relation input catalogs" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}

    relation =
      RelationRef.new!(%{
        connection: :runner_sql_runtime,
        catalog: "int",
        schema: "sales",
        name: "customers_normalized"
      })

    raw_relation =
      RelationRef.new!(%{
        connection: :runner_sql_runtime,
        catalog: "raw",
        schema: "crm",
        name: "customers"
      })

    version =
      register_sql_manifest!(ref, relation, [
        %RelationInput{
          kind: :plain_relation,
          relation_ref: raw_relation,
          raw: "raw.crm.customers"
        }
      ])

    work = %RunnerWork{
      run_id: "run_sql_relation_input_catalog_scope",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert_received {:connect_opts, :runner_sql_runtime, opts}
    assert Keyword.fetch!(opts, :required_catalogs) == ["int", "raw"]
  end

  test "preview and explain scope SQL sessions to declared relation input catalogs" do
    asset = %{
      type: :sql,
      module: FavnRunner.ExecutionSQLAssetTest.PlainRelationInputSQLAsset
    }

    assert {:ok, _preview} = Favn.SQLAsset.Runtime.preview(asset)
    assert_received {:connect_opts, :runner_sql_runtime, preview_opts}
    assert Keyword.fetch!(preview_opts, :required_catalogs) == ["int", "raw"]

    assert {:ok, _explain} = Favn.SQLAsset.Runtime.explain(asset)
    assert_received {:connect_opts, :runner_sql_runtime, explain_opts}
    assert Keyword.fetch!(explain_opts, :required_catalogs) == ["int", "raw"]
  end

  test "executes manifest-pinned sql asset through declared runner SQL runtime" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_in_process",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref,
        execution_package: execution_package_for(version)
      }

    assert {:ok, result} = FavnRunner.run(work)
    if result.status != :ok, do: flunk(inspect(result, pretty: true))
    assert [%{status: :ok}] = result.asset_results
  end

  test "manifest execution binds Favn-owned run identity and start time" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    sql = "SELECT @favn_run_id AS run_id, @favn_run_started_at AS started_at"
    run_started_at = ~U[2026-07-17 08:30:00Z]

    version =
      register_sql_manifest!(ref, nil, [], SessionRequirements.empty(), sql)

    work = %RunnerWork{
      run_id: "run_sql_favn_runtime_inputs",
      run_started_at: run_started_at,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }

    assert {:ok, %{status: :ok}} = FavnRunner.run(work)
    assert_received {:materialize_params, ["run_sql_favn_runtime_inputs", ^run_started_at]}
  end

  test "resolves and pins manifest-declared runtime inputs before execution" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver
      )

    node_identity =
      NodeIdentity.new!(%{
        manifest_version_id: version.manifest_version_id,
        node_key: {ref, nil},
        target_refs: [ref],
        planned_asset_refs: [ref]
      })

    work = %RunnerWork{
      run_id: "run_sql_runtime_inputs",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version),
      node_identity: node_identity,
      params: %{submitted: 7}
    }

    assert {:ok, resolution} = FavnRunner.resolve_runtime_inputs(work)

    assert_received {:runtime_inputs_context, context}
    assert context.run_id == work.run_id
    assert context.asset.ref == ref
    assert context.node_identity == node_identity
    assert context.params == %{submitted: 7}
    refute_received {:connect_after_runtime_inputs, _resolved?}

    pinned = %{work | runtime_input_pin: Pin.new(work.run_id, {ref, nil}, resolution)}
    assert {:ok, result} = FavnRunner.run(pinned)
    assert result.status == :ok

    assert_received {:connect_after_runtime_inputs, true}
    assert_received {:materialize_params, ["runtime-value", 7]}
    refute_received {:runtime_inputs_context, _context}

    assert [asset_result] = result.asset_results

    assert asset_result.meta.runtime_inputs == %{
             resolver: FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver,
             input_identity: "manifest:runtime-inputs",
             input_metadata: %{file_count: 1},
             duration_ms: asset_result.meta.runtime_inputs.duration_ms
           }

    refute inspect(asset_result.meta) =~ "runtime-value"
  end

  test "resolver failure prevents SQL rendering and connection mutation" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsFailureSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsFailureResolver
      )

    work = work_for(version, ref, "run_sql_runtime_inputs_failure")

    assert {:error,
            %RunnerError{
              type: :runtime_inputs_failed,
              retryable?: true,
              outcome: :safe_failure
            }} =
             FavnRunner.resolve_runtime_inputs(work)

    refute_received {:connect_opts, :runner_sql_runtime, _opts}
    refute_received {:materialize_params, _params}
  end

  test "known-safe SQL connection failures carry an explicit safe outcome" do
    reload_fake_connection(
      :runner_sql_runtime,
      FavnRunner.ExecutionSQLAssetTest.FakeRetryableConnectErrorAdapter
    )

    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_sql_safe_connect_failure"))
    assert result.status == :error

    assert %RunnerError{retryable?: true, outcome: :safe_failure} = result.error

    assert [
             %ResourceOutcome{
               resource: %ResourceRef{kind: :connection, name: "runner_sql_runtime"},
               status: :failure,
               category: :connection_error,
               safe_to_repeat?: true
             }
           ] = result.error.resource_outcomes

    assert [%{error: %RunnerError{retryable?: true, outcome: :safe_failure}}] =
             result.asset_results
  end

  test "manifest execution refuses runtime inputs that were not pinned first" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver
      )

    assert {:ok, result} =
             FavnRunner.run(work_for(version, ref, "run_sql_runtime_inputs_unpinned"))

    assert result.status == :error
    assert [%{error: %{type: :runtime_inputs_invalid_result}}] = result.asset_results
    refute_received {:runtime_inputs_context, _context}
    refute_received {:connect_opts, :runner_sql_runtime, _opts}
    refute_received {:materialize_params, _params}
  end

  test "resolver receives the final planned runtime window" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver
      )

    requested_start = ~U[2026-07-14 00:00:00Z]
    requested_end = ~U[2026-07-15 00:00:00Z]
    anchor_key = Favn.Window.Key.new!(:day, requested_start, "Etc/UTC")

    requested_window =
      Favn.Window.Runtime.new!(
        :day,
        requested_start,
        requested_end,
        anchor_key,
        timezone: "Etc/UTC"
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_window")
      |> Map.put(:params, %{submitted: 7})
      |> Map.put(:trigger, %{window: requested_window})

    work = resolve_and_pin!(work)
    assert {:ok, %{status: :ok}} = FavnRunner.run(work)
    assert_received {:runtime_inputs_context, context}
    assert context.window.start_at == requested_start
    assert context.window.end_at == requested_end
    assert context.window.anchor_key == anchor_key
  end

  test "incremental runtime-input resolution receives the exact planned node window" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}
    window_spec = Favn.Window.Spec.new!(:day, lookback: 1, timezone: "Etc/UTC")

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver,
        materialization: {:incremental, strategy: :delete_insert, unique_key: [:id]},
        window: window_spec
      )

    requested_start = ~U[2026-07-14 00:00:00Z]
    requested_end = ~U[2026-07-15 00:00:00Z]
    anchor_key = Favn.Window.Key.new!(:day, requested_start, "Etc/UTC")

    requested_window =
      Favn.Window.Runtime.new!(
        :day,
        requested_start,
        requested_end,
        anchor_key,
        timezone: "Etc/UTC"
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_lookback")
      |> Map.put(:trigger, %{window: requested_window})

    assert {:ok, _resolution} = FavnRunner.resolve_runtime_inputs(work)
    assert_received {:runtime_inputs_context, context}
    assert context.window.start_at == requested_start
    assert context.window.end_at == requested_end
    assert context.window.anchor_key == anchor_key
    assert work.trigger.window == requested_window
  end

  test "planner lookback nodes reach runner resolution as distinct exact windows" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}
    window_spec = Favn.Window.Spec.new!(:month, lookback: 1, timezone: "Europe/Oslo")

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver,
        materialization:
          {:incremental, strategy: :delete_insert, window_column: :partition_month},
        window: window_spec
      )

    {:ok, graph_index} = GraphIndex.build_index(version.manifest.assets)

    anchor =
      Favn.Window.Anchor.new!(
        :month,
        oslo_datetime!(~N[2026-07-01 00:00:00]),
        oslo_datetime!(~N[2026-08-01 00:00:00]),
        timezone: "Europe/Oslo"
      )

    assert {:ok, plan} =
             Planner.plan(ref, graph_index: graph_index, anchor_window: anchor)

    windows =
      plan.nodes
      |> Map.values()
      |> Enum.map(& &1.window)
      |> Enum.sort_by(&DateTime.to_unix(&1.start_at, :microsecond))

    assert Enum.map(windows, & &1.start_at) == [
             oslo_datetime!(~N[2026-06-01 00:00:00]),
             oslo_datetime!(~N[2026-07-01 00:00:00])
           ]

    Enum.with_index(windows, fn window, index ->
      work =
        version
        |> work_for(ref, "run_sql_planner_window_#{index}")
        |> Map.put(:trigger, %{window: window})

      assert {:ok, _resolution} = FavnRunner.resolve_runtime_inputs(work)
      assert_received {:runtime_inputs_context, context}
      assert context.window == window
    end)
  end

  test "monthly runtime-input resolution preserves the exact Oslo node window" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}
    timezone = "Europe/Oslo"
    database = Favn.Timezone.database!()
    window_spec = Favn.Window.Spec.new!(:month, lookback: 1, timezone: timezone)

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver,
        materialization: {:incremental, strategy: :delete_insert, unique_key: [:id]},
        window: window_spec
      )

    requested_start = DateTime.from_naive!(~N[2026-04-01 00:00:00], timezone, database)
    requested_end = DateTime.from_naive!(~N[2026-05-01 00:00:00], timezone, database)
    anchor_key = Favn.Window.Key.new!(:month, requested_start, timezone)

    requested_window =
      Favn.Window.Runtime.new!(
        :month,
        requested_start,
        requested_end,
        anchor_key,
        timezone: timezone
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_monthly_lookback")
      |> Map.put(:trigger, %{window: requested_window})

    assert Calendar.get_time_zone_database() == Calendar.UTCOnlyTimeZoneDatabase
    assert {:ok, _resolution} = FavnRunner.resolve_runtime_inputs(work)
    assert_received {:runtime_inputs_context, context}
    assert context.window.start_at == requested_start
    assert context.window.end_at == requested_end
    assert context.window.anchor_key == anchor_key
    assert work.trigger.window == requested_window
  end

  test "UTC monthly runtime-input resolution preserves the exact node window" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}
    window_spec = Favn.Window.Spec.new!(:month, lookback: 1, timezone: "Etc/UTC")

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver,
        materialization: {:incremental, strategy: :delete_insert, unique_key: [:id]},
        window: window_spec
      )

    requested_start = ~U[2026-06-01 00:00:00Z]
    requested_end = ~U[2026-07-01 00:00:00Z]
    anchor_key = Favn.Window.Key.new!(:month, requested_start, "Etc/UTC")

    requested_window =
      Favn.Window.Runtime.new!(
        :month,
        requested_start,
        requested_end,
        anchor_key,
        timezone: "Etc/UTC"
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_utc_monthly_lookback")
      |> Map.put(:trigger, %{window: requested_window})

    assert {:ok, _resolution} = FavnRunner.resolve_runtime_inputs(work)
    assert_received {:runtime_inputs_context, context}
    assert context.window.start_at == requested_start
    assert context.window.end_at == requested_end
    assert context.window.anchor_key == anchor_key
    assert work.trigger.window == requested_window
  end

  test "runtime-input resolution honors the remaining work deadline" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.SlowRuntimeInputsResolver
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_deadline")
      |> Map.put(:deadline_at, DateTime.add(DateTime.utc_now(), 40, :millisecond))

    started_at = System.monotonic_time(:millisecond)

    assert {:error, %RunnerError{type: :runtime_inputs_timeout, outcome: :unknown}} =
             FavnRunner.resolve_runtime_inputs(work)

    assert System.monotonic_time(:millisecond) - started_at < 400
  end

  test "runtime-input boundary redacts sensitive values from pin lineage" do
    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.SensitiveLineageRuntimeInputsResolver
      )

    work = work_for(version, ref, "run_sql_runtime_inputs_sensitive_lineage")

    assert {:ok, resolution} = FavnRunner.resolve_runtime_inputs(work)
    assert resolution.input_identity == "[REDACTED]"
    assert resolution.metadata == %{echo: :redacted, nested: ["prefix-[REDACTED]"]}
    refute inspect(resolution) =~ "lineage-secret"

    pin = Pin.new(work.run_id, {ref, nil}, resolution)
    lineage = Pin.lineage(pin)

    assert lineage.input_identity == "[REDACTED]"
    refute inspect(pin) =~ "lineage-secret"
    refute inspect(lineage) =~ "lineage-secret"
  end

  test "sensitive resolved values are redacted from adapter failures" do
    reload_fake_connection(
      :runner_sql_runtime,
      FavnRunner.ExecutionSQLAssetTest.FakeRuntimeInputSecretAdapter
    )

    ref = {FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset, :asset}

    version =
      register_runtime_input_sql_manifest!(
        ref,
        FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver
      )

    work =
      version
      |> work_for(ref, "run_sql_runtime_inputs_redaction")
      |> Map.put(:params, %{submitted: 7})
      |> resolve_and_pin!()

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    refute inspect(result, limit: :infinity) =~ "runtime-value"
    assert inspect(result, limit: :infinity) =~ "[REDACTED]"
  end

  test "shares resolved parameters with transactional SQL checks" do
    reload_fake_connection(
      :runner_sql_runtime,
      FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter
    )

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}

    check =
      checked_check(
        :runtime_input_check,
        :before_materialize,
        :fail,
        "SELECT @runtime_value = @runtime_value AS passed"
      )

    version =
      register_checked_sql_manifest!(
        ref,
        [check],
        %RuntimeInputResolverRef{
          module: FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver
        }
      )

    work =
      version
      |> work_for(ref, "run_checked_runtime_inputs")
      |> resolve_and_pin!()

    assert {:ok, %{status: :ok}} = FavnRunner.run(work)

    assert_received {:checked_query_params, ["runtime-value", "runtime-value"]}
  end

  test "elixir asset SQLClient sessions inherit owned relation catalog scope" do
    configure_public_fake_connection!()

    ref = {FavnRunner.ExecutionSQLAssetTest.ElixirSQLClientAsset, :asset}

    relation =
      RelationRef.new!(%{
        connection: :runner_sql_runtime,
        catalog: "raw",
        schema: "sales",
        name: "raw_ingestion_asset"
      })

    version = register_elixir_manifest!(ref, relation)

    work = %RunnerWork{
      run_id: "run_elixir_sql_catalog_scope",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref
    }

    assert {:ok, result} = FavnRunner.run(work)
    if result.status != :ok, do: flunk(inspect(result, pretty: true))
    assert_received {:connect_opts, :runner_sql_runtime, opts}
    assert Keyword.fetch!(opts, :required_catalogs) == ["raw"]
  end

  test "manifest execution does not fall back to compiled modules for deferred refs" do
    ref = {FavnRunner.ExecutionSQLAssetTest.ManifestOnlySQLAsset, :asset}
    version = register_manifest_with_missing_relation!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_manifest_only",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref,
        execution_package: execution_package_for(version)
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert [asset_result] = result.asset_results
    assert asset_result.status == :error
    assert %{type: :unresolved_asset_ref} = asset_result.error
  end

  test "manifest execution fails when the work package is missing" do
    ref = {FavnRunner.ExecutionSQLAssetTest.MissingPayloadSQLAsset, :asset}
    version = register_sql_manifest!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_missing_payload",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref
      }

    assert {:error, :execution_package_required} = FavnRunner.run(work)
  end

  test "manifest sql execution preflights missing runtime connection before execution" do
    Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)

    ref = {FavnRunner.ExecutionSQLAssetTest.MissingConnectionSQLAsset, :asset}
    version = register_sql_manifest!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_missing_connection",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref,
        execution_package: execution_package_for(version)
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert result.asset_results == []
    assert result.error.type == :missing_runtime_config
    assert result.error.phase == :sql_preflight

    assert [%{type: :missing_connection, connection: :runner_sql_runtime}] =
             result.error.details.errors
  end

  test "manifest sql execution redacts backend error details and causes" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeSecretExecutionAdapter)

    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    work = %RunnerWork{
      run_id: "run_sql_secret_failure",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    assert [asset_result] = result.asset_results

    refute inspect(asset_result.error) =~ "super-secret"
    refute inspect(asset_result.error) =~ "user:password"
    refute inspect(asset_result.error) =~ "credential=raw"
    assert asset_result.error.details.cause.details.password == :redacted

    assert [
             %ResourceOutcome{
               resource: %ResourceRef{kind: :connection, name: "runner_sql_runtime"},
               status: :success,
               category: :sql_resource_reached
             }
           ] = result.resource_outcomes
  end

  test "checked materialization stages once, persists warnings, and commits" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    attach_check_telemetry()

    checks = [
      checked_check(
        :candidate_ready,
        :before_materialize,
        :fail,
        "select true as passed, 2 as row_count from query() /* check:pass */"
      ),
      checked_check(
        :target_warning,
        :after_materialize,
        :warn,
        "select false as passed, 1 as invalid_count from target() /* check:warn */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_warning"))
    assert result.status == :ok
    assert [asset_result] = result.asset_results
    assert asset_result.meta.quality_status == :warning
    assert asset_result.meta.write_outcome == :written

    assert Enum.map(asset_result.meta.check_results, &{&1.name, &1.outcome}) == [
             {:candidate_ready, :passed},
             {:target_warning, :warned}
           ]

    assert_received {:checked_execute, stage_statement}
    assert stage_statement =~ "CREATE TEMP TABLE"
    assert_received {:checked_materialize, write_plan}
    assert IO.iodata_to_binary(write_plan.select_sql) =~ "favn_check_candidate_"
    assert_received :checked_transaction_commit

    assert_received {:check_telemetry, %{duration_ms: duration_ms}, telemetry}
    assert is_integer(duration_ms)
    assert telemetry.check == :candidate_ready
    assert telemetry.outcome == :passed
    assert telemetry.transaction_outcome == :committed
    assert telemetry.write_outcome == :written

    assert_received {:check_telemetry, _measurements, telemetry}
    assert telemetry.check == :target_warning
    assert telemetry.outcome == :warned
    assert telemetry.transaction_outcome == :committed
  end

  test "checked incremental materialization inspects non-empty and empty candidates as relations" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    Application.put_env(:favn_runner, :checked_columns, [
      %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: false},
      %Column{name: "partition_month", position: 2, data_type: "DATE", nullable?: false}
    ])

    check =
      checked_check(
        :candidate_valid,
        :before_materialize,
        :fail,
        "select count(*) >= 0 as passed from query() /* check:pass */"
      )

    contract =
      Contract.new!(%{
        columns: [
          %{name: :id, type: :integer, null: false},
          %{name: :partition_month, type: :date, null: false}
        ]
      })

    materialization =
      {:incremental, strategy: :delete_insert, window_column: :partition_month}

    window_spec = Favn.Window.Spec.new!(:month, timezone: "Etc/UTC")
    start_at = ~U[2026-07-01 00:00:00Z]
    end_at = ~U[2026-08-01 00:00:00Z]

    runtime_window =
      Favn.Window.Runtime.new!(
        :month,
        start_at,
        end_at,
        Favn.Window.Key.new!(:month, start_at, "Etc/UTC"),
        timezone: "Etc/UTC"
      )

    for {name, sql} <- [
          {:non_empty, "SELECT 1 AS id, DATE '2026-07-01' AS partition_month"},
          {:empty, "SELECT 1 AS id, DATE '2026-07-01' AS partition_month WHERE false"}
        ] do
      ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}

      version =
        register_checked_sql_manifest!(ref, [check], nil, contract,
          sql: sql,
          materialization: materialization,
          window: window_spec
        )

      work =
        version
        |> work_for(ref, "run_checked_incremental_#{name}")
        |> Map.put(:trigger, %{window: runtime_window})

      assert {:ok, result} = FavnRunner.run(work)
      assert result.status == :ok
      assert [%{meta: %{write_outcome: :written}}] = result.asset_results

      assert_received {:checked_columns,
                       %RelationRef{name: "favn_check_candidate_" <> _rest} = candidate}

      assert_received {:checked_columns, ^candidate}

      assert_received {:checked_materialize, write_plan}
      assert write_plan.strategy == :delete_insert
      refute_received :checked_incremental_probe
    end
  end

  test "missing candidate delete-scope column rolls back before target mutation" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    Application.put_env(:favn_runner, :checked_columns, [
      %Column{name: "id", position: 1, data_type: "INTEGER", nullable?: false}
    ])

    check =
      checked_check(
        :candidate_valid,
        :before_materialize,
        :fail,
        "select true as passed from query() /* check:pass */"
      )

    materialization =
      {:incremental, strategy: :delete_insert, window_column: :partition_month}

    window_spec = Favn.Window.Spec.new!(:month, timezone: "Etc/UTC")
    start_at = ~U[2026-07-01 00:00:00Z]

    runtime_window =
      Favn.Window.Runtime.new!(
        :month,
        start_at,
        ~U[2026-08-01 00:00:00Z],
        Favn.Window.Key.new!(:month, start_at, "Etc/UTC"),
        timezone: "Etc/UTC"
      )

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedFailureSQLAsset, :asset}

    version =
      register_checked_sql_manifest!(ref, [check], nil, nil,
        sql: "SELECT 1 AS id",
        materialization: materialization,
        window: window_spec
      )

    work =
      version
      |> work_for(ref, "run_checked_incremental_missing_scope")
      |> Map.put(:trigger, %{window: runtime_window})

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert [
             %{
               error: %{
                 type: :materialization_planning_failed,
                 message: "incremental delete scope column is missing"
               },
               meta: %{write_outcome: :rolled_back}
             }
           ] = result.asset_results

    assert_received :checked_transaction_rollback
    refute_received {:checked_materialize, _write_plan}
    refute_received :checked_incremental_probe
  end

  test "a failed after check rolls back and persists failed-attempt diagnostics" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    attach_check_telemetry()

    checks = [
      checked_check(
        :target_valid,
        :after_materialize,
        :fail,
        "select false as passed, 4 as invalid_count from target() /* check:fail */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_failure"))
    assert result.status == :error
    assert [asset_result] = result.asset_results
    assert asset_result.meta.quality_status == :failed
    assert asset_result.meta.write_outcome == :rolled_back
    assert [%{name: :target_valid, outcome: :failed}] = asset_result.meta.check_results
    assert asset_result.error.type == :check_failed
    assert_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_rollback
    refute_received :checked_transaction_commit

    assert_received {:check_telemetry, _measurements, telemetry}
    assert telemetry.check == :target_valid
    assert telemetry.outcome == :failed
    assert telemetry.transaction_outcome == :rolled_back
    assert telemetry.write_outcome == :rolled_back
  end

  test "rollback failures retain diagnostics and mark the transaction outcome unknown" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_rollback_failure, true)
    attach_check_telemetry()

    checks = [
      checked_check(
        :target_valid,
        :after_materialize,
        :fail,
        "select false as passed from target() /* check:fail */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedRollbackFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_rollback_failure"))
    assert result.status == :error
    assert [%{meta: meta}] = result.asset_results
    assert meta.transaction_outcome == :unknown
    assert meta.write_outcome == :unknown
    assert [%{name: :target_valid, outcome: :failed}] = meta.check_results
    assert_received :checked_transaction_rollback

    assert_received {:check_telemetry, _measurements, telemetry}
    assert telemetry.transaction_outcome == :unknown
    assert telemetry.write_outcome == :unknown
  end

  test "transaction begin failures are reported as not started" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_begin_failure, true)

    checks = [
      checked_check(
        :candidate_valid,
        :before_materialize,
        :fail,
        "select true as passed from query() /* check:pass */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedBeginFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_begin_failure"))
    assert result.status == :error
    assert [%{meta: meta}] = result.asset_results
    assert meta.transaction_outcome == :not_started
    assert meta.write_outcome == :not_started

    assert [
             %{
               name: :candidate_valid,
               outcome: :not_run,
               reason: :transaction_not_started
             }
           ] = meta.check_results

    refute_received {:checked_query, _statement}
    refute_received {:checked_materialize, _write_plan}
  end

  test "a skip check commits a successful no-op and marks later checks not run" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    checks = [
      checked_check(
        :unchanged,
        :before_materialize,
        :skip_materialization,
        "select false as passed from target() /* check:skip */",
        when: :target_exists
      ),
      checked_check(
        :after_write,
        :after_materialize,
        :fail,
        "select true as passed from target() /* check:pass */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSkipSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_skip"))
    assert result.status == :ok
    assert [asset_result] = result.asset_results
    assert asset_result.meta.write_outcome == :no_op
    assert asset_result.meta.quality_status == :warning
    assert asset_result.meta.reason == :unchanged

    assert Enum.map(asset_result.meta.check_results, &{&1.name, &1.outcome}) == [
             {:unchanged, :materialization_skipped},
             {:after_write, :not_run}
           ]

    refute_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_commit
  end

  test "validates a contract candidate schema before writing and persists evidence" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    contract = Contract.new!(%{columns: [%{name: :id, type: :integer, null: false}]})
    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_contract_schema"))
    assert [%{status: :ok, meta: meta}] = result.asset_results
    assert meta.contract_validation.status == :passed
    assert [%{name: "id", type: :integer}] = meta.contract_validation.expected_columns
    assert [%{name: "id", native_type: "INTEGER"}] = meta.contract_validation.observed_columns
    assert_received {:checked_columns, %Favn.RelationRef{name: "favn_check_candidate_" <> _rest}}
    assert_received {:checked_materialize, _write_plan}
  end

  test "validates parameterized row-count inputs before opening a SQL session" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    contract =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_counts: [[equals: Param.new!(:expected_rows)]]
      })

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    assert {:ok, missing_result} =
             FavnRunner.run(work_for(version, ref, "run_contract_param_missing"))

    assert missing_result.status == :error
    assert [%{error: %{type: :missing_query_param}}] = missing_result.asset_results
    refute_received :checked_connect
    refute_received {:checked_execute, _statement}

    work =
      version
      |> work_for(ref, "run_contract_param_valid")
      |> Map.put(:params, %{expected_rows: 1})

    assert {:ok, valid_result} = FavnRunner.run(work)
    assert valid_result.status == :ok
    assert_received :checked_connect
    assert_received {:checked_query_params, [1, 1]}
  end

  test "an earlier row-count failure cannot become a later successful no-op" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_contract_outcomes, [false, false])

    contract = ordered_row_count_contract()
    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    work =
      version
      |> work_for(ref, "run_ordered_row_count_failure")
      |> Map.put(:params, %{expected_rows: 100})

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    assert [%{meta: meta}] = result.asset_results
    assert meta.write_outcome == :rolled_back

    assert Enum.map(meta.check_results, &{&1.claim_id, &1.outcome}) == [
             {"row_count.equals.param.expected_rows", :failed},
             {"row_count.min.1", :not_run}
           ]

    assert_received {:checked_query, _statement}
    refute_received {:checked_query, _statement}
    refute_received {:checked_materialize, _write_plan}
  end

  test "a later row-count no-op preserves the target after exact reconciliation passes" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_contract_outcomes, [true, false])

    contract = ordered_row_count_contract()
    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSkipSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    work =
      version
      |> work_for(ref, "run_ordered_row_count_no_op")
      |> Map.put(:params, %{expected_rows: 0})

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{meta: meta}] = result.asset_results
    assert meta.write_outcome == :no_op

    assert Enum.map(meta.check_results, &{&1.claim_id, &1.outcome}) == [
             {"row_count.equals.param.expected_rows", :passed},
             {"row_count.min.1", :materialization_skipped}
           ]

    refute_received {:checked_materialize, _write_plan}
  end

  test "a target-conditioned row-count claim is skipped during empty bootstrap" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_target_exists, false)
    Application.put_env(:favn_runner, :checked_contract_outcomes, [true])

    contract = ordered_row_count_contract()
    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedBootstrapSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    work =
      version
      |> work_for(ref, "run_ordered_row_count_bootstrap")
      |> Map.put(:params, %{expected_rows: 0})

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{meta: meta}] = result.asset_results
    assert meta.write_outcome == :written

    assert Enum.map(meta.check_results, &{&1.claim_id, &1.outcome}) == [
             {"row_count.equals.param.expected_rows", :passed},
             {"row_count.min.1", :condition_skipped}
           ]

    assert_received {:checked_materialize, _write_plan}
  end

  test "rejects caller-supplied Favn runtime names before opening a SQL session" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, nil)

    work =
      version
      |> work_for(ref, "run_reserved_runtime_param")
      |> Map.put(:params, %{"favn_run_id" => "spoofed-run"})

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    assert [%{error: %{type: :binding_failure, details: details}}] = result.asset_results
    assert details.details == %{name: :favn_run_id, source: :params}
    refute inspect(result) =~ "spoofed-run"
    refute_received :checked_connect
  end

  test "parameterized row counts accept pinned runtime-input resolver values" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    contract =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_counts: [[equals: Param.new!(:expected_rows)]]
      })

    resolver = %RuntimeInputResolverRef{
      module: FavnRunner.ExecutionSQLAssetTest.ExpectedRowsResolver
    }

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], resolver, contract)

    work =
      version
      |> work_for(ref, "run_contract_param_resolved")
      |> resolve_and_pin!()

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert_received {:checked_query_params, [1, 1]}
  end

  test "fails a schema mismatch before target mutation with structured differences" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    Application.put_env(:favn_runner, :checked_columns, [
      %Column{name: "other", position: 1, data_type: "VARCHAR", nullable?: true}
    ])

    contract = Contract.new!(%{columns: [%{name: :id, type: :integer, null: false}]})
    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, [], nil, contract)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_contract_mismatch"))
    assert result.status == :error
    assert [%{error: %{type: :contract_violation}, meta: meta}] = result.asset_results
    assert meta.contract_validation.status == :failed

    assert Enum.any?(
             meta.contract_validation.differences,
             &match?(%{kind: :missing, column: "id"}, &1)
           )

    refute_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_rollback
  end

  test "a target-existence check is condition-skipped during bootstrap and the write commits" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_target_exists, false)

    checks = [
      checked_check(
        :existing_target_valid,
        :before_materialize,
        :fail,
        "select true as passed from target() /* check:pass */",
        when: :target_exists
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedBootstrapSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_bootstrap"))
    assert result.status == :ok
    assert [%{meta: meta}] = result.asset_results
    assert [%{name: :existing_target_valid, outcome: :condition_skipped}] = meta.check_results
    assert meta.write_outcome == :written
    assert_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_commit
  end

  test "SQL errors and invalid check result shapes roll back with durable diagnostics" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)

    for {name, marker, expected_reason} <- [
          {:check_sql_error, "check:sql_error", :backend_execution_failed},
          {:check_invalid_result, "check:invalid", :invalid_check_result}
        ] do
      checks = [
        checked_check(
          name,
          :before_materialize,
          :fail,
          "select true as passed from query() /* #{marker} */"
        )
      ]

      ref = {FavnRunner.ExecutionSQLAssetTest.CheckedInvalidSQLAsset, :asset}
      version = register_checked_sql_manifest!(ref, checks)

      assert {:ok, result} =
               FavnRunner.run(work_for(version, ref, "run_checked_#{expected_reason}"))

      assert result.status == :error
      assert [%{meta: meta, error: error}] = result.asset_results
      assert [%{outcome: :errored, reason: ^expected_reason}] = meta.check_results
      assert meta.write_outcome == :rolled_back
      assert error.phase == :before_materialize
      assert_received :checked_transaction_rollback
    end
  end

  test "candidate cleanup failure rolls back the write" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedExecutionAdapter)
    Application.put_env(:favn_runner, :checked_cleanup_failure, true)

    checks = [
      checked_check(
        :candidate_valid,
        :before_materialize,
        :fail,
        "select true as passed from query() /* check:pass */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedCleanupSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_cleanup"))
    assert result.status == :error
    assert [%{meta: %{write_outcome: :rolled_back}}] = result.asset_results
    assert_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_rollback
    refute_received :checked_transaction_commit
  end

  test "commit failures retain check diagnostics without surfacing SQL or bound params" do
    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeCheckedCommitErrorAdapter)

    checks = [
      checked_check(
        :candidate_valid,
        :before_materialize,
        :fail,
        "select true as passed from query() /* check:pass */"
      )
    ]

    ref = {FavnRunner.ExecutionSQLAssetTest.CheckedCommitFailureSQLAsset, :asset}
    version = register_checked_sql_manifest!(ref, checks)

    assert {:ok, result} = FavnRunner.run(work_for(version, ref, "run_checked_commit_failure"))
    assert result.status == :error
    assert [%{error: error, meta: meta}] = result.asset_results
    assert [%{name: :candidate_valid, outcome: :passed}] = meta.check_results

    surfaced = inspect(error, limit: :infinity)
    refute surfaced =~ "commit-bound-secret"
    refute surfaced =~ "commit-sql-secret"
    refute surfaced =~ "transaction_body_result"
  end

  test "inspection normalizes malformed include values at the runner boundary" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      asset_ref: ref,
      include: nil
    }

    assert {:ok, result} = FavnRunner.Inspection.inspect_relation(request, version)
    assert result.asset_ref == ref
    assert result.relation_ref.name == "manifest_sql_asset"
    assert result.relation == nil
    assert result.columns == []
    assert result.row_count == nil
    assert result.sample == nil
    assert result.table_metadata == %{}
    assert result.warnings == []
  end

  test "inspection rejects invalid sample limits before opening a SQL session" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      asset_ref: ref,
      include: [:sample],
      sample_limit: -1
    }

    assert {:error, :invalid_sample_limit} =
             FavnRunner.Inspection.inspect_relation(request, version)
  end

  test "inspection warnings expose adapter messages without error details or causes" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    relation = RelationRef.new!(%{connection: :inspection_fake, name: "orders"})

    :ok =
      Registry.reload(
        %{
          inspection_fake: %Resolved{
            name: :inspection_fake,
            adapter: FavnRunner.ExecutionSQLAssetTest.FakeInspectionAdapter,
            module: __MODULE__,
            config: %{}
          }
        },
        registry_name: FavnRunner.ConnectionRegistry
      )

    version = register_sql_manifest!(ref, relation)

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      asset_ref: ref,
      include: [:row_count]
    }

    assert {:ok, result} = FavnRunner.Inspection.inspect_relation(request, version)
    assert [%{code: :row_count_failed, message: "safe row count failure"}] = result.warnings
  end

  defp register_sql_manifest!(
         ref,
         relation \\ nil,
         relation_inputs \\ [],
         session_requirements \\ SessionRequirements.empty(),
         sql \\ "SELECT 1 AS id"
       ) do
    relation =
      relation || RelationRef.new!(%{connection: :runner_sql_runtime, name: "manifest_sql_asset"})

    template =
      Template.compile!(sql,
        file: "test/sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    execution = %SQLExecution{sql: sql, template: template, sql_definitions: []}
    package = execution_package!(ref, execution)

    manifest =
      %Manifest{
        schema_version: 9,
        runner_contract_version: 9,
        assets: [
          %Asset{
            ref: ref,
            module: elem(ref, 0),
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1},
            relation: relation,
            materialization: :table,
            relation_inputs: relation_inputs,
            session_requirements: session_requirements,
            execution_package_hash: package.content_hash,
            assurance: assurance(execution)
          }
        ],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
        metadata: %{}
      }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_sql_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    remember_execution_package(version, package)
  end

  defp register_runtime_input_sql_manifest!(ref, resolver, opts \\ []) do
    relation =
      RelationRef.new!(%{connection: :runner_sql_runtime, name: "runtime_input_sql_asset"})

    sql = "SELECT @runtime_value AS value, @submitted AS submitted"
    materialization = Keyword.get(opts, :materialization, :table)
    window = Keyword.get(opts, :window)

    template =
      Template.compile!(sql,
        file: "test/runtime_input_sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    execution = %SQLExecution{
      sql: sql,
      template: template,
      runtime_inputs: %RuntimeInputResolverRef{module: resolver},
      sql_definitions: []
    }

    package = execution_package!(ref, execution)

    manifest = %Manifest{
      schema_version: 9,
      runner_contract_version: 9,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation: relation,
          window: window,
          materialization: materialization,
          execution_package_hash: package.content_hash,
          assurance: assurance(execution)
        }
      ],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_runtime_input_sql_" <>
            Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    remember_execution_package(version, package)
  end

  defp register_checked_sql_manifest!(
         ref,
         checks,
         runtime_inputs \\ nil,
         contract \\ nil,
         opts \\ []
       ) do
    relation =
      RelationRef.new!(%{connection: :runner_sql_runtime, schema: "gold", name: "checked_asset"})

    sql = Keyword.get(opts, :sql, "SELECT 1 AS id")

    template =
      Template.compile!(sql,
        file: "test/checked_sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    checks = generated_contract_checks(contract) ++ checks

    execution = %SQLExecution{
      sql: sql,
      template: template,
      runtime_inputs: runtime_inputs,
      contract: contract,
      sql_definitions: [],
      checks: checks
    }

    package = execution_package!(ref, execution)

    manifest = %Manifest{
      schema_version: 9,
      runner_contract_version: 9,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation: relation,
          window: Keyword.get(opts, :window),
          materialization: Keyword.get(opts, :materialization, :table),
          execution_package_hash: package.content_hash,
          assurance: assurance(execution)
        }
      ],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_checked_sql_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    remember_execution_package(version, package)
  end

  defp generated_contract_checks(nil), do: []

  defp generated_contract_checks(%Contract{} = contract) do
    Enum.map(Contract.generated_check_specs(contract), fn spec ->
      template =
        Template.compile!(spec.sql,
          file: "test/checked_sql_contract_manifest.sql",
          line: 1,
          module: __MODULE__,
          scope: :query,
          enforce_query_root: true
        )

      Check.new!(%{
        name: spec.name,
        at: spec.at,
        on_violation: spec.on_violation,
        when: spec.when,
        message: spec.message,
        sql: spec.sql,
        template: template,
        origin: :contract,
        claim_id: spec.claim_id,
        uses_query?: true,
        uses_target?: false
      })
    end)
  end

  defp ordered_row_count_contract do
    Contract.new!(%{
      columns: [%{name: :id, type: :integer}],
      row_counts: [
        [equals: Param.new!(:expected_rows), on_violation: :fail],
        [min: 1, when: :target_exists, on_violation: :skip_materialization]
      ]
    })
  end

  defp checked_check(name, at, on_violation, sql, opts \\ []) do
    template =
      Template.compile!(sql,
        file: "test/checked_sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    runtime_relations = Template.runtime_relations(template)

    Check.new!(%{
      name: name,
      at: at,
      on_violation: on_violation,
      when: Keyword.get(opts, :when),
      message: Keyword.get(opts, :message),
      sql: sql,
      template: template,
      file: "test/checked_sql_asset_manifest.sql",
      line: 1,
      uses_query?: MapSet.member?(runtime_relations, :query),
      uses_target?: MapSet.member?(runtime_relations, :target)
    })
  end

  defp work_for(version, ref, run_id) do
    %RunnerWork{
      run_id: run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: ref,
      execution_package: execution_package_for(version)
    }
  end

  defp oslo_datetime!(naive) do
    DateTime.from_naive!(naive, "Europe/Oslo", Favn.Timezone.database!())
  end

  defp resolve_and_pin!(%RunnerWork{} = work) do
    {:ok, resolution} = FavnRunner.resolve_runtime_inputs(work)
    node_key = RunnerWork.node_key(work) || {work.asset_ref, nil}
    %{work | runtime_input_pin: Pin.new(work.run_id, node_key, resolution)}
  end

  defp attach_check_telemetry do
    handler_id = "#{inspect(__MODULE__)}-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :sql_asset, :check],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:check_telemetry, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp register_manifest_with_missing_relation!(ref) do
    deferred_module =
      Module.concat([__MODULE__, "HiddenSource#{System.unique_integer([:positive])}"])

    relation =
      RelationRef.new!(%{connection: :runner_sql_runtime, name: "manifest_sql_asset_missing"})

    template =
      Template.compile!("SELECT * FROM #{inspect(deferred_module)}",
        file: "test/sql_asset_manifest_missing_ref.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    Code.compile_string(
      """
      defmodule #{inspect(deferred_module)} do
      end
      """,
      "test/dynamic_manifest_hidden_source_asset.exs"
    )

    execution = %SQLExecution{
      sql: "SELECT * FROM #{inspect(deferred_module)}",
      template: template,
      sql_definitions: []
    }

    package = execution_package!(ref, execution)

    manifest =
      %Manifest{
        schema_version: 9,
        runner_contract_version: 9,
        assets: [
          %Asset{
            ref: ref,
            module: elem(ref, 0),
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1},
            relation: relation,
            materialization: :view,
            execution_package_hash: package.content_hash,
            assurance: assurance(execution)
          }
        ],
        pipelines: [],
        schedules: [],
        graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
        metadata: %{}
      }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_sql_missing_ref_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    remember_execution_package(version, package)
  end

  defp register_elixir_manifest!(ref, relation) do
    manifest = %Manifest{
      schema_version: 9,
      runner_contract_version: 9,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :elixir,
          execution: %{entrypoint: :asset, arity: 1},
          relation: relation
        }
      ],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [ref], edges: [], topo_order: [ref]},
      metadata: %{}
    }

    {:ok, version} =
      Version.new(manifest,
        manifest_version_id:
          "mv_elixir_sql_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    version
  end

  defp execution_package!(ref, %SQLExecution{} = execution) do
    {:ok, package} = ExecutionPackage.new(ref, execution)
    package
  end

  defp assurance(%SQLExecution{contract: nil, checks: []}), do: nil

  defp assurance(%SQLExecution{} = execution) do
    %{
      contract: execution.contract,
      checks:
        Enum.map(execution.checks, fn check ->
          Map.take(check, [:name, :origin, :claim_id, :at, :when, :on_violation, :message])
        end)
    }
  end

  defp remember_execution_package(%Version{} = version, %ExecutionPackage{} = package) do
    Process.put({:execution_package, version.manifest_version_id}, package)
    version
  end

  defp execution_package_for(%Version{} = version) do
    Process.get({:execution_package, version.manifest_version_id})
  end

  defp reload_fake_connection(name, adapter) when is_atom(name) and is_atom(adapter) do
    :ok =
      Registry.reload(
        %{
          name => %Resolved{
            name: name,
            adapter: adapter,
            module: __MODULE__,
            config: %{}
          }
        },
        registry_name: FavnRunner.ConnectionRegistry
      )
  end

  defp configure_public_fake_connection! do
    previous_modules = Application.get_env(:favn, :connection_modules, :unset)
    previous_connections = Application.get_env(:favn, :connections, :unset)

    Application.put_env(:favn, :connection_modules, [__MODULE__.FakeConnection])
    Application.put_env(:favn, :connections, runner_sql_runtime: [])

    on_exit(fn ->
      restore_app_env(:connection_modules, previous_modules)
      restore_app_env(:connections, previous_connections)
    end)
  end

  defp restore_app_env(key, :unset), do: Application.delete_env(:favn, key)
  defp restore_app_env(key, value), do: Application.put_env(:favn, key, value)

  defp restore_env(key, nil), do: Application.delete_env(:favn_runner, key)
  defp restore_env(key, value), do: Application.put_env(:favn_runner, key, value)
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.RuntimeInputsSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.RuntimeInputsFailureSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.RuntimeInputsResolver do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(context) do
    Application.put_env(:favn_runner, :runtime_inputs_resolved, true)

    if pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid) do
      send(pid, {:runtime_inputs_context, context})
    end

    {:ok,
     %Result{
       params: %{runtime_value: "runtime-value"},
       identity: "manifest:runtime-inputs",
       metadata: %{file_count: 1},
       sensitive_params: [:runtime_value]
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.RuntimeInputsFailureResolver do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Error

  @impl true
  def resolve(_context) do
    {:error,
     %Error{
       reason: :not_ready,
       message: "external manifest is not ready",
       retryable?: true
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.ExpectedRowsResolver do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(_context) do
    {:ok, %Result{params: %{expected_rows: 1}, identity: "expected-rows:1"}}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.SlowRuntimeInputsResolver do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(_context) do
    Process.sleep(500)
    {:ok, %Result{params: %{runtime_value: "late"}, identity: "late"}}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.SensitiveLineageRuntimeInputsResolver do
  @behaviour Favn.SQLAsset.RuntimeInputs

  alias Favn.SQLAsset.RuntimeInputs.Result

  @impl true
  def resolve(_context) do
    {:ok,
     %Result{
       params: %{runtime_value: "lineage-secret"},
       identity: "lineage-secret",
       metadata: %{echo: "lineage-secret", nested: ["prefix-lineage-secret"]},
       sensitive_params: [:runtime_value]
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.PlainRelationInputSQLAsset do
  use Favn.SQLAsset

  relation(
    connection: :runner_sql_runtime,
    catalog: "int",
    schema: "sales",
    name: "customers_normalized"
  )

  materialized(:table)

  query do
    ~SQL"""
    select customer_id
    from raw.crm.customers
    """
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingPayloadSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingConnectionSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.CheckedSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.CheckedFailureSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.CheckedSkipSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.ElixirSQLClientAsset do
  alias Favn.SQL.Client, as: SQLClient

  def asset(ctx) do
    with {:ok, session} <- SQLClient.connect(ctx.asset.relation.connection) do
      :ok = SQLClient.disconnect(session)
      {:ok, %{}}
    end
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeConnection do
  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :runner_sql_runtime,
      adapter: FavnRunner.ExecutionSQLAssetTest.FakeExecutionAdapter,
      config_schema: [%{key: :noop, default: nil}]
    }
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeInspectionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Error

  def connect(%Resolved{} = resolved, opts) do
    if pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid) do
      send(pid, {:connect_opts, resolved.name, opts})
    end

    {:ok, :conn}
  end

  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def row_count(:conn, _ref, _opts) do
    {:error,
     %Error{
       type: :execution_error,
       message: "safe row count failure",
       operation: :row_count,
       details: %{password: "do-not-leak"},
       cause: %{token: "do-not-leak"}
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Result

  def connect(%Resolved{} = resolved, opts) do
    if pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid) do
      send(pid, {:connect_opts, resolved.name, opts})

      send(
        pid,
        {:connect_after_runtime_inputs,
         Application.get_env(:favn_runner, :runtime_inputs_resolved)}
      )
    end

    {:ok, :conn}
  end

  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def query(:conn, _statement, _opts),
    do: {:ok, %Result{kind: :query, command: "SELECT", rows: [], columns: []}}

  def materialize(:conn, _write_plan, opts) do
    if pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid) do
      send(pid, {:materialize_params, Keyword.get(opts, :params, [])})
    end

    {:ok, %Result{command: :insert, rows_affected: 1}}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeRetryableConnectErrorAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Error

  def connect(%Resolved{}, _opts) do
    {:error,
     %Error{
       type: :connection_error,
       message: "connection failed before SQL execution",
       operation: :connect,
       retryable?: true
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeSecretExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Error

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def materialize(:conn, _write_plan, _opts) do
    {:error,
     %Error{
       type: :execution_error,
       message: "failed against postgres://user:password@example/db?token=super-secret",
       operation: :materialize,
       connection: :runner_sql_runtime,
       details: %{password: "super-secret", nested: %{reason: "credential=raw"}},
       cause: %{token: "super-secret"}
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeRuntimeInputSecretAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Error

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def materialize(:conn, _write_plan, opts) do
    secret = opts |> Keyword.fetch!(:params) |> hd()

    {:error,
     %Error{
       type: :execution_error,
       message: "adapter rejected #{secret}",
       operation: :materialize,
       details: %{echo: secret},
       cause: {:rejected, secret}
     }}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Error, Relation, Result}

  def connect(%Resolved{}, _opts) do
    notify(:checked_connect)
    {:ok, :checked_conn}
  end

  def disconnect(:checked_conn, _opts), do: :ok

  def capabilities(%Resolved{}, _opts),
    do: {:ok, %Capabilities{transactions: :supported, replace_table: :supported}}

  def relation(:checked_conn, ref, _opts) do
    if Application.get_env(:favn_runner, :checked_target_exists, true) do
      {:ok,
       %Relation{
         catalog: ref.catalog,
         schema: ref.schema,
         name: ref.name,
         type: :table
       }}
    else
      {:ok, nil}
    end
  end

  def columns(:checked_conn, ref, _opts) do
    notify({:checked_columns, ref})
    {:ok, Application.fetch_env!(:favn_runner, :checked_columns)}
  end

  def execute(:checked_conn, statement, _opts) do
    statement = IO.iodata_to_binary(statement)
    notify({:checked_execute, statement})

    if String.starts_with?(statement, "DROP TABLE") and
         Application.get_env(:favn_runner, :checked_cleanup_failure, false) do
      {:error, %Error{type: :execution_error, message: "candidate cleanup failed"}}
    else
      {:ok, %Result{kind: :execute, command: "EXECUTE", rows_affected: 0}}
    end
  end

  def query(:checked_conn, statement, opts) do
    statement = IO.iodata_to_binary(statement)
    notify({:checked_query, statement})
    notify({:checked_query_params, Keyword.get(opts, :params, [])})

    cond do
      String.contains?(statement, "favn_incremental_probe") ->
        notify(:checked_incremental_probe)

        {:ok,
         %Result{
           kind: :query,
           command: "SELECT",
           columns: [],
           rows: []
         }}

      String.contains?(statement, "check:sql_error") ->
        {:error, %Error{type: :execution_error, message: "check query failed"}}

      String.contains?(statement, "check:invalid") ->
        {:ok,
         %Result{
           kind: :query,
           command: "SELECT",
           columns: ["passed"],
           rows: [%{"passed" => 1}]
         }}

      true ->
        passed? = checked_query_passed?(statement)

        {:ok,
         %Result{
           kind: :query,
           command: "SELECT",
           columns: ["passed", "row_count"],
           rows: [%{"passed" => passed?, "row_count" => 1}]
         }}
    end
  end

  defp checked_query_passed?(statement) do
    if String.contains?(statement, "favn_contract_row_count") do
      case Application.get_env(:favn_runner, :checked_contract_outcomes, []) do
        [passed? | remaining] ->
          Application.put_env(:favn_runner, :checked_contract_outcomes, remaining)
          passed?

        [] ->
          true
      end
    else
      not (String.contains?(statement, "check:warn") or
             String.contains?(statement, "check:fail") or
             String.contains?(statement, "check:skip"))
    end
  end

  def materialize_in_transaction(:checked_conn, write_plan, _opts) do
    notify({:checked_materialize, write_plan})
    {:ok, %Result{kind: :materialize, command: "INSERT", rows_affected: 1}}
  end

  def transaction(:checked_conn, fun, _opts) do
    notify(:checked_transaction_begin)

    if Application.get_env(:favn_runner, :checked_begin_failure, false) do
      {:error,
       %Error{
         type: :connection_failed,
         message: "transaction begin failed",
         operation: :transaction,
         details: %{transaction_stage: :begin}
       }}
    else
      case fun.(:checked_conn) do
        {:ok, value} ->
          notify(:checked_transaction_commit)
          {:ok, value}

        {:error, %Error{} = error} ->
          notify(:checked_transaction_rollback)

          if Application.get_env(:favn_runner, :checked_rollback_failure, false) do
            {:error,
             %Error{
               type: :execution_error,
               message: "transaction rollback failed",
               operation: :transaction,
               details: %{transaction_stage: :rollback},
               cause: error
             }}
          else
            {:error, error}
          end
      end
    end
  end

  defp notify(message) do
    if pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid) do
      send(pid, message)
    end
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeCheckedCommitErrorAdapter do
  alias Favn.SQL.Error

  defdelegate connect(resolved, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate disconnect(conn, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate capabilities(resolved, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate relation(conn, ref, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate execute(conn, statement, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate query(conn, statement, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  defdelegate materialize_in_transaction(conn, write_plan, opts),
    to: FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter

  def transaction(:checked_conn, fun, _opts) do
    case fun.(:checked_conn) do
      {:ok, value} ->
        secret_plan = %{
          value.write_plan
          | select_sql: "select 'commit-sql-secret'",
            params: ["commit-bound-secret"]
        }

        secret_value = %{value | write_plan: secret_plan}

        {:error,
         %Error{
           type: :execution_error,
           message: "commit failed",
           operation: :transaction,
           details: %{
             classification: :unknown_commit_state,
             transaction_body_result: secret_value
           }
         }}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end
end
