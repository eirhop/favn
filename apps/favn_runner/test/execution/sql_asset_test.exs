defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Asset.RelationInput
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Check
  alias Favn.SQL.Template

  setup do
    previous_test_pid = Application.get_env(:favn_runner, :execution_sql_asset_test_pid)
    previous_target_exists = Application.get_env(:favn_runner, :checked_target_exists)
    previous_cleanup_failure = Application.get_env(:favn_runner, :checked_cleanup_failure)
    previous_rollback_failure = Application.get_env(:favn_runner, :checked_rollback_failure)
    previous_begin_failure = Application.get_env(:favn_runner, :checked_begin_failure)
    Application.put_env(:favn_runner, :execution_sql_asset_test_pid, self())
    Application.put_env(:favn_runner, :checked_target_exists, true)
    Application.put_env(:favn_runner, :checked_cleanup_failure, false)
    Application.put_env(:favn_runner, :checked_rollback_failure, false)
    Application.put_env(:favn_runner, :checked_begin_failure, false)

    on_exit(fn ->
      restore_env(:execution_sql_asset_test_pid, previous_test_pid)
      restore_env(:checked_target_exists, previous_target_exists)
      restore_env(:checked_cleanup_failure, previous_cleanup_failure)
      restore_env(:checked_rollback_failure, previous_rollback_failure)
      restore_env(:checked_begin_failure, previous_begin_failure)
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
      asset_ref: ref
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert_received {:connect_opts, :runner_sql_runtime, opts}
    assert Keyword.fetch!(opts, :required_catalogs) == ["raw"]
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
      asset_ref: ref
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
        asset_ref: ref
      }

    assert {:ok, result} = FavnRunner.run(work)
    if result.status != :ok, do: flunk(inspect(result, pretty: true))
    assert [%{status: :ok}] = result.asset_results
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
        asset_ref: ref
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert [asset_result] = result.asset_results
    assert asset_result.status == :error
    assert %{type: :unresolved_asset_ref} = asset_result.error
  end

  test "manifest execution fails when sql payload is missing" do
    ref = {FavnRunner.ExecutionSQLAssetTest.MissingPayloadSQLAsset, :asset}
    version = register_manifest_without_sql_execution!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_missing_payload",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error

    assert [asset_result] = result.asset_results
    assert asset_result.status == :error
    assert %{type: :invalid_sql_asset_definition, phase: :runtime} = asset_result.error
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
        asset_ref: ref
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
      asset_ref: ref
    }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :error
    assert [asset_result] = result.asset_results

    refute inspect(asset_result.error) =~ "super-secret"
    refute inspect(asset_result.error) =~ "user:password"
    refute inspect(asset_result.error) =~ "credential=raw"
    assert asset_result.error.details.cause.details.password == :redacted
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
    assert asset_result.meta.reason == :unchanged

    assert Enum.map(asset_result.meta.check_results, &{&1.name, &1.outcome}) == [
             {:unchanged, :materialization_skipped},
             {:after_write, :not_run}
           ]

    refute_received {:checked_materialize, _write_plan}
    assert_received :checked_transaction_commit
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

    version = register_inspection_manifest!(ref, relation)

    request = %RelationInspectionRequest{
      manifest_version_id: version.manifest_version_id,
      asset_ref: ref,
      include: [:row_count]
    }

    assert {:ok, result} = FavnRunner.Inspection.inspect_relation(request, version)
    assert [%{code: :row_count_failed, message: "safe row count failure"}] = result.warnings
  end

  defp register_inspection_manifest!(ref, relation) do
    manifest = %Manifest{
      schema_version: 3,
      runner_contract_version: 3,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation: relation,
          materialization: :table,
          sql_execution: nil
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
          "mv_inspection_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    version
  end

  defp register_sql_manifest!(ref, relation \\ nil, relation_inputs \\ []) do
    relation =
      relation || RelationRef.new!(%{connection: :runner_sql_runtime, name: "manifest_sql_asset"})

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    manifest =
      %Manifest{
        schema_version: 3,
        runner_contract_version: 3,
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
            sql_execution: %SQLExecution{
              sql: "SELECT 1 AS id",
              template: template,
              sql_definitions: []
            }
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
    version
  end

  defp register_checked_sql_manifest!(ref, checks) do
    relation =
      RelationRef.new!(%{connection: :runner_sql_runtime, schema: "gold", name: "checked_asset"})

    template =
      Template.compile!("SELECT 1 AS id",
        file: "test/checked_sql_asset_manifest.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    manifest = %Manifest{
      schema_version: 3,
      runner_contract_version: 3,
      assets: [
        %Asset{
          ref: ref,
          module: elem(ref, 0),
          name: :asset,
          type: :sql,
          execution: %{entrypoint: :asset, arity: 1},
          relation: relation,
          materialization: :table,
          sql_execution: %SQLExecution{
            sql: "SELECT 1 AS id",
            template: template,
            sql_definitions: [],
            checks: checks
          }
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
    version
  end

  defp checked_check(name, at, on_false, sql, opts \\ []) do
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
      on_false: on_false,
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
      asset_ref: ref
    }
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

    manifest =
      %Manifest{
        schema_version: 3,
        runner_contract_version: 3,
        assets: [
          %Asset{
            ref: ref,
            module: elem(ref, 0),
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1},
            relation: relation,
            materialization: :view,
            sql_execution: %SQLExecution{
              sql: "SELECT * FROM #{inspect(deferred_module)}",
              template: template,
              sql_definitions: []
            }
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
    version
  end

  defp register_manifest_without_sql_execution!(ref) do
    relation =
      RelationRef.new!(%{connection: :runner_sql_runtime, name: "manifest_sql_asset_missing"})

    manifest =
      %Manifest{
        schema_version: 3,
        runner_contract_version: 3,
        assets: [
          %Asset{
            ref: ref,
            module: elem(ref, 0),
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1},
            relation: relation,
            materialization: :view,
            sql_execution: nil
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
          "mv_sql_missing_payload_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
      )

    :ok = FavnRunner.register_manifest(version)
    version
  end

  defp register_elixir_manifest!(ref, relation) do
    manifest = %Manifest{
      schema_version: 3,
      runner_contract_version: 3,
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

defmodule FavnRunner.ExecutionSQLAssetTest.PlainRelationInputSQLAsset do
  use Favn.Namespace,
    relation: [connection: :runner_sql_runtime, catalog: "int", schema: "sales"]

  use Favn.SQLAsset

  @relation [name: "customers_normalized"]
  @materialized :table
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
    end

    {:ok, :conn}
  end

  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def query(:conn, _statement, _opts),
    do: {:ok, %Result{kind: :query, command: "SELECT", rows: [], columns: []}}

  def materialize(:conn, _write_plan, _opts),
    do: {:ok, %Result{command: :insert, rows_affected: 1}}
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

defmodule FavnRunner.ExecutionSQLAssetTest.FakeCheckedExecutionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Error, Relation, Result}

  def connect(%Resolved{}, _opts), do: {:ok, :checked_conn}
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

  def query(:checked_conn, statement, _opts) do
    statement = IO.iodata_to_binary(statement)
    notify({:checked_query, statement})

    cond do
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
        passed? =
          not (String.contains?(statement, "check:warn") or
                 String.contains?(statement, "check:fail") or
                 String.contains?(statement, "check:skip"))

        {:ok,
         %Result{
           kind: :query,
           command: "SELECT",
           columns: ["passed", "row_count"],
           rows: [%{"passed" => passed?, "row_count" => 1}]
         }}
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
