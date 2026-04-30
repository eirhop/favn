defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Template

  setup do
    previous_runner_plugins = Application.get_env(:favn, :runner_plugins)

    on_exit(fn ->
      if is_nil(previous_runner_plugins) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous_runner_plugins)
      end

      Registry.reload(%{}, registry_name: FavnRunner.ConnectionRegistry)
    end)

    reload_fake_connection(:runner_sql_runtime, __MODULE__.FakeExecutionAdapter)

    :ok
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
    assert result.status == :ok
    assert [%{status: :ok}] = result.asset_results
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

  test "manifest sql execution reports backend failure when runtime connection is missing" do
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

    assert [asset_result] = result.asset_results
    assert asset_result.status == :error
    assert %{type: :backend_execution_failed, phase: :materialize} = asset_result.error
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
      schema_version: 1,
      runner_contract_version: 1,
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

  defp register_sql_manifest!(ref) do
    relation = RelationRef.new!(%{connection: :runner_sql_runtime, name: "manifest_sql_asset"})

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
        schema_version: 1,
        runner_contract_version: 1,
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
        schema_version: 1,
        runner_contract_version: 1,
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
        schema_version: 1,
        runner_contract_version: 1,
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
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingPayloadSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingConnectionSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.FakeInspectionAdapter do
  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Error

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
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

  def connect(%Resolved{}, _opts), do: {:ok, :conn}
  def disconnect(:conn, _opts), do: :ok
  def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

  def materialize(:conn, _write_plan, _opts),
    do: {:ok, %Result{command: :insert, rows_affected: 1}}
end
