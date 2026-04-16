defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Registry
  alias Favn.Connection.Resolved
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
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
end
