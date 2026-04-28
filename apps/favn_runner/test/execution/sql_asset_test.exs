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

      worker_module = Module.concat([FavnDuckdb, Worker])

      if Process.whereis(worker_module) do
        GenServer.stop(worker_module, :normal, 1_000)
      end
    end)

    :ok =
      Registry.reload(
        %{
          duckdb_runtime: %Resolved{
            name: :duckdb_runtime,
            adapter: Favn.SQL.Adapter.DuckDB,
            module: __MODULE__,
            config: %{database: ":memory:"}
          }
        },
        registry_name: FavnRunner.ConnectionRegistry
      )

    :ok
  end

  test "executes manifest-pinned sql asset in in_process mode" do
    plugin_module = Module.concat([FavnDuckdb])
    Application.put_env(:favn, :runner_plugins, [{plugin_module, execution_mode: :in_process}])

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

  test "executes manifest-pinned sql asset in separate_process mode" do
    plugin_module = Module.concat([FavnDuckdb])
    worker_module = Module.concat([FavnDuckdb, Worker])
    client_module = Module.concat([Favn, SQL, Adapter, DuckDB, Client, Duckdbex])

    Application.put_env(:favn, :runner_plugins, [
      {plugin_module, execution_mode: :separate_process, worker_name: worker_module}
    ])

    {:ok, _pid} =
      worker_module.start_link(name: worker_module, client: client_module)

    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}
    version = register_sql_manifest!(ref)

    work =
      %RunnerWork{
        run_id: "run_sql_separate_process",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{status: :ok}] = result.asset_results
  end

  test "manifest execution does not fall back to compiled modules for deferred refs" do
    plugin_module = Module.concat([FavnDuckdb])
    Application.put_env(:favn, :runner_plugins, [{plugin_module, execution_mode: :in_process}])

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
    plugin_module = Module.concat([FavnDuckdb])
    Application.put_env(:favn, :runner_plugins, [{plugin_module, execution_mode: :in_process}])

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
    plugin_module = Module.concat([FavnDuckdb])
    Application.put_env(:favn, :runner_plugins, [{plugin_module, execution_mode: :in_process}])

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

  defp register_sql_manifest!(ref) do
    relation = RelationRef.new!(%{connection: :duckdb_runtime, name: "manifest_sql_asset"})

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
      RelationRef.new!(%{connection: :duckdb_runtime, name: "manifest_sql_asset_missing"})

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
        use Favn.Asset

        def asset(_ctx), do: :ok
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
      RelationRef.new!(%{connection: :duckdb_runtime, name: "manifest_sql_asset_missing"})

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
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingPayloadSQLAsset do
end

defmodule FavnRunner.ExecutionSQLAssetTest.MissingConnectionSQLAsset do
end
