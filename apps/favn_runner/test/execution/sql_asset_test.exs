defmodule FavnRunner.ExecutionSQLAssetTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version

  setup do
    resolved = %Resolved{
      name: :warehouse,
      adapter: FavnRunner.ExecutionSQLAssetTest.DummyAdapter,
      module: FavnRunner.ExecutionSQLAssetTest.DummyConnection,
      config: %{database: ":memory:"},
      required_keys: [:database],
      secret_fields: [],
      schema_keys: [:database],
      metadata: %{}
    }

    :ok =
      Favn.Connection.Registry.reload(%{warehouse: resolved},
        registry_name: FavnRunner.ConnectionRegistry
      )

    :ok
  end

  test "runs one sql asset through moved runner sql runtime" do
    ref = {FavnRunner.ExecutionSQLAssetTest.SQLAsset, :asset}

    manifest =
      %Manifest{
        schema_version: 1,
        runner_contract_version: 1,
        assets: [
          %Asset{
            ref: ref,
            module: FavnRunner.ExecutionSQLAssetTest.SQLAsset,
            name: :asset,
            type: :sql,
            execution: %{entrypoint: :asset, arity: 1}
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

    assert :ok = FavnRunner.register_manifest(version)

    work =
      %RunnerWork{
        run_id: "run_sql_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower),
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: ref,
        params: %{}
      }

    assert {:ok, result} = FavnRunner.run(work)
    assert result.status == :ok
    assert [%{status: :ok, meta: meta}] = result.asset_results
    assert meta[:materialized] == :table
    assert meta[:connection] == :warehouse
    assert meta[:rows_affected] == 1
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.DummyConnection do
end

defmodule FavnRunner.ExecutionSQLAssetTest.DummyAdapter do
  @behaviour Favn.SQL.Adapter

  alias Favn.RelationRef
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Column
  alias Favn.SQL.Relation
  alias Favn.SQL.Result
  alias Favn.SQL.WritePlan

  @impl true
  def connect(_resolved, _opts), do: {:ok, :dummy_conn}

  @impl true
  def disconnect(_conn, _opts), do: :ok

  @impl true
  def capabilities(_resolved, _opts), do: {:ok, %Capabilities{transactions: :supported}}

  @impl true
  def execute(_conn, statement, _opts) do
    {:ok, %Result{kind: :execute, command: IO.iodata_to_binary(statement), rows_affected: 0}}
  end

  @impl true
  def query(_conn, statement, _opts) do
    {:ok,
     %Result{
       kind: :query,
       command: IO.iodata_to_binary(statement),
       rows_affected: 1,
       rows: [%{"value" => 1}],
       columns: ["value"]
     }}
  end

  @impl true
  def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

  @impl true
  def materialization_statements(_plan, _caps, _opts), do: {:ok, ["SELECT 1"]}

  @impl true
  def ping(_conn, _opts), do: :ok

  @impl true
  def schema_exists?(_conn, _schema, _opts), do: {:ok, true}

  @impl true
  def relation(_conn, %RelationRef{} = ref, _opts) do
    {:ok, %Relation{catalog: ref.catalog, schema: ref.schema, name: ref.name, type: :table}}
  end

  @impl true
  def list_schemas(_conn, _opts), do: {:ok, ["main"]}

  @impl true
  def list_relations(_conn, _schema, _opts), do: {:ok, []}

  @impl true
  def columns(_conn, %RelationRef{} = _ref, _opts), do: {:ok, [%Column{name: "value"}]}

  @impl true
  def transaction(conn, fun, _opts), do: fun.(conn)

  @impl true
  def materialize(_conn, %WritePlan{} = _plan, _opts) do
    {:ok, %Result{kind: :materialize, command: "materialize", rows_affected: 1}}
  end
end

defmodule FavnRunner.ExecutionSQLAssetTest.SQLAsset do
  alias Favn.Asset
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Definition

  @spec __favn_sql_asset_definition__() :: Definition.t()
  def __favn_sql_asset_definition__ do
    sql = "select 1 as value"

    template =
      Template.compile!(sql,
        file: "nofile",
        line: 1,
        module: __MODULE__,
        scope: :query,
        local_args: [],
        enforce_query_root: true
      )

    relation = RelationRef.new!(%{connection: :warehouse, schema: "main", name: "example"})

    asset = %Asset{
      module: __MODULE__,
      name: :asset,
      entrypoint: :asset,
      ref: Ref.new(__MODULE__, :asset),
      arity: 1,
      type: :sql,
      file: "nofile",
      line: 1,
      relation: relation,
      materialization: :table,
      config: %{},
      depends_on: [],
      dependencies: [],
      meta: %{},
      relation_inputs: []
    }

    %Definition{
      module: __MODULE__,
      asset: asset,
      sql: sql,
      template: template,
      materialization: :table,
      relation_inputs: [],
      sql_definitions: [],
      raw_asset: %{sql_file: "nofile"}
    }
  end
end
