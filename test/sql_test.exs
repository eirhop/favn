defmodule Favn.SQLTest do
  use ExUnit.Case

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.SQL
  alias Favn.SQL.{Capabilities, Error, Relation, RelationRef, Result, WritePlan}

  defmodule ConnectionProvider do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :sql_runtime,
        adapter: Favn.SQLTest.Adapter,
        config_schema: [%{key: :database, required: true, type: :string}]
      }
    end
  end

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{} = _resolved, _opts), do: {:ok, :adapter_conn}

    @impl true
    def disconnect(:adapter_conn, _opts), do: :ok

    @impl true
    def capabilities(_resolved, _opts) do
      {:ok, %Capabilities{replace_view: :supported, transactions: :emulated}}
    end

    @impl true
    def execute(:adapter_conn, statement, _opts) do
      {:ok,
       %Result{
         kind: :execute,
         command: IO.iodata_to_binary(statement),
         rows_affected: 1,
         metadata: %{}
       }}
    end

    @impl true
    def query(:adapter_conn, statement, _opts) do
      sql = IO.iodata_to_binary(statement)

      rows =
        case sql do
          "schema_exists" -> [%{"schema" => "main"}]
          "relation_lookup" -> [%Relation{name: "orders", schema: "main", type: :table}]
          "list_schemas" -> [%{"schema" => "main"}, %{"schema" => "staging"}]
          "list_relations" -> [%Relation{name: "orders", schema: "main", type: :table}]
          "list_columns" -> [%Favn.SQL.Column{name: "id", data_type: "integer"}]
          _ -> []
        end

      {:ok, %Result{kind: :query, command: sql, rows_affected: nil, rows: rows}}
    end

    @impl true
    def introspection_query(:schema_exists, _schema, _opts), do: {:ok, "schema_exists"}

    def introspection_query(:relation, %RelationRef{}, _opts), do: {:ok, "relation_lookup"}

    def introspection_query(:list_schemas, nil, _opts), do: {:ok, "list_schemas"}

    def introspection_query(:list_relations, _schema, _opts), do: {:ok, "list_relations"}

    def introspection_query(:columns, %RelationRef{}, _opts), do: {:ok, "list_columns"}

    @impl true
    def materialization_statements(%WritePlan{}, %Capabilities{}, _opts),
      do: {:ok, ["prep", "write", "post"]}
  end

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state)
      Favn.Connection.Registry.reload(%{})
    end)

    Application.put_env(:favn, :connection_modules, [ConnectionProvider])
    Application.put_env(:favn, :connections, sql_runtime: [database: "local"])

    {:ok, resolved} = Favn.Connection.Loader.load()
    :ok = Favn.Connection.Registry.reload(resolved)
    :ok
  end

  test "connect resolves connection and returns runtime session" do
    assert {:ok, session} = SQL.connect(:sql_runtime)
    assert session.resolved.name == :sql_runtime
    assert %Capabilities{} = session.capabilities
    assert :ok = SQL.disconnect(session)
  end

  test "introspection fallback goes through adapter-provided introspection_query/3" do
    assert {:ok, session} = SQL.connect(:sql_runtime)

    assert {:ok, true} = SQL.schema_exists?(session, "main")

    assert {:ok, %Relation{name: "orders", type: :table}} =
             SQL.get_relation(session, %RelationRef{schema: "main", name: "orders"})

    assert {:ok, ["main", "staging"]} = SQL.list_schemas(session)

    assert {:ok, [%Relation{}]} = SQL.list_relations(session, "main")

    assert {:ok, [%Favn.SQL.Column{name: "id"}]} =
             SQL.columns(session, %RelationRef{schema: "main", name: "orders"})
  end

  test "materialize fallback executes adapter-provided statements" do
    assert {:ok, session} = SQL.connect(:sql_runtime)

    plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "orders", type: :table},
        select_sql: "select 1"
      }

    assert {:ok, %Result{kind: :materialize, command: "post"}} = SQL.materialize(session, plan)
  end

  test "connect returns normalized missing connection error" do
    assert {:error, %Error{type: :invalid_config, connection: :unknown}} = SQL.connect(:unknown)
  end
end
