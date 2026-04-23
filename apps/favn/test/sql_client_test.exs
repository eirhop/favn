defmodule Favn.SQLClientTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Result
  alias Favn.SQL.WritePlan

  defmodule TestConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :test_sql,
        adapter: Favn.SQLClientTest.Adapter,
        config_schema: [
          %{key: :database, required: true, type: :string}
        ]
      }
    end
  end

  defmodule Adapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :conn}

    @impl true
    def disconnect(:conn, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts),
      do: {:ok, %Favn.SQL.Capabilities{transactions: :supported}}

    @impl true
    def execute(:conn, _statement, _opts), do: {:ok, %Result{kind: :execute, rows_affected: 1}}

    @impl true
    def query(:conn, statement, _opts),
      do: {:ok, %Result{kind: :query, command: IO.iodata_to_binary(statement)}}

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "SELECT 1"}

    @impl true
    def materialization_statements(%WritePlan{}, %Favn.SQL.Capabilities{}, _opts), do: {:ok, []}

    @impl true
    def materialize(:conn, %WritePlan{}, _opts), do: {:ok, %Result{kind: :materialize}}

    @impl true
    def relation(:conn, %RelationRef{name: name, schema: schema}, _opts),
      do: {:ok, %Favn.SQL.Relation{name: name, schema: schema || "main", type: :table}}

    @impl true
    def columns(:conn, %RelationRef{}, _opts), do: {:ok, [%Favn.SQL.Column{name: "id"}]}

    @impl true
    def transaction(:conn, fun, _opts), do: fun.(:conn)
  end

  setup do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)

    Application.put_env(:favn, :connection_modules, [TestConnection])
    Application.put_env(:favn, :connections, test_sql: [database: ":memory:"])

    on_exit(fn ->
      restore_env(:connection_modules, previous_modules)
      restore_env(:connections, previous_connections)
    end)

    :ok
  end

  test "connects and runs SQL operations without runner runtime" do
    assert {:ok, session} = Favn.SQLClient.connect(:test_sql)
    assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(session, "select 1")

    assert {:ok, %Result{kind: :execute}} =
             Favn.SQLClient.execute(session, "create table t(id int)")

    assert {:ok, %Favn.SQL.Capabilities{transactions: :supported}} =
             Favn.SQLClient.capabilities(session)

    assert {:ok, %Favn.SQL.Relation{name: "orders"}} =
             Favn.SQLClient.relation(session, name: :orders)

    assert {:ok, [%Favn.SQL.Column{name: "id"}]} =
             Favn.SQLClient.columns(session, %{name: "orders"})

    assert :ok = Favn.SQLClient.disconnect(session)
  end

  test "with_connection wraps connect and disconnect" do
    assert {:ok, :done} =
             Favn.SQLClient.with_connection(:test_sql, [], fn session ->
               assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(session, "select 1")
               {:ok, :done}
             end)
  end

  test "transaction delegates through adapter callback" do
    assert {:ok, session} = Favn.SQLClient.connect(:test_sql)

    assert {:ok, :inside} =
             Favn.SQLClient.transaction(session, fn tx_session ->
               assert {:ok, %Result{kind: :query}} = Favn.SQLClient.query(tx_session, "select 1")
               {:ok, :inside}
             end)
  end

  test "returns invalid_config for unknown connection" do
    assert {:error, %Favn.SQL.Error{type: :invalid_config, operation: :connect}} =
             Favn.SQLClient.connect(:missing)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
