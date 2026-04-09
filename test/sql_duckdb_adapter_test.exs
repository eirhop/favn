defmodule Favn.SQLDuckDBAdapterTest do
  use ExUnit.Case

  alias Favn.Connection.Definition
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry
  alias Favn.SQL
  alias Favn.SQL.{Error, Relation, RelationRef, Result, WritePlan}

  defmodule DuckDBConnectionProvider do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :duckdb_runtime,
        adapter: Favn.SQL.Adapter.DuckDB,
        config_schema: [%{key: :database, required: true, type: :string}]
      }
    end
  end

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state)
      Favn.Connection.Registry.reload(%{})
    end)

    Application.put_env(:favn, :connection_modules, [DuckDBConnectionProvider])
    Application.put_env(:favn, :connections, duckdb_runtime: [database: ":memory:"])

    {:ok, resolved} = Loader.load()
    :ok = Registry.reload(resolved)

    {:ok, session} = SQL.connect(:duckdb_runtime)

    on_exit(fn ->
      :ok = SQL.disconnect(session)
    end)

    {:ok, session: session}
  end

  test "connect/query/execute with parameter binding", %{session: session} do
    assert {:ok, %Result{kind: :execute}} =
             SQL.execute(session, "CREATE TABLE metrics(id INTEGER, value INTEGER)")

    assert {:ok, %Result{kind: :execute}} =
             SQL.execute(session, "INSERT INTO metrics VALUES ($1, $2)", params: [1, 42])

    assert {:ok, %Result{kind: :query, rows: [%{"id" => 1, "value" => 42}]}} =
             SQL.query(session, "SELECT id, value FROM metrics WHERE id = $1", params: [1])
  end

  test "introspection callbacks return normalized values", %{session: session} do
    assert {:ok, %Result{}} =
             SQL.execute(
               session,
               "CREATE TABLE introspection_orders(order_id INTEGER, amount DOUBLE)"
             )

    assert {:ok, true} = SQL.schema_exists?(session, "main")

    assert {:ok, %Relation{name: "introspection_orders", type: :table}} =
             SQL.get_relation(session, %RelationRef{schema: "main", name: "introspection_orders"})

    assert {:ok, schemas} = SQL.list_schemas(session)
    assert "main" in schemas

    assert {:ok, relations} = SQL.list_relations(session, "main")

    assert Enum.any?(relations, fn relation ->
             relation.name == "introspection_orders" and relation.type == :table
           end)

    assert {:ok, columns} =
             SQL.columns(session, %RelationRef{schema: "main", name: "introspection_orders"})

    assert Enum.map(columns, & &1.name) == ["order_id", "amount"]
  end

  test "materialize table via appender rows", %{session: session} do
    plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "bulk_users", type: :table},
        select_sql: "SELECT 1::INTEGER AS id, 'seed'::VARCHAR AS name",
        options: %{appender_rows: [[2, "alpha"], [3, "beta"]]}
      }

    assert {:ok, %Result{kind: :materialize, command: "appender", rows_affected: 2}} =
             SQL.materialize(session, plan)

    assert {:ok, %Result{rows: rows}} =
             SQL.query(session, "SELECT id, name FROM bulk_users ORDER BY id")

    assert rows == [%{"id" => 2, "name" => "alpha"}, %{"id" => 3, "name" => "beta"}]
  end

  test "appender table path preserves non-replacing table semantics", %{session: session} do
    plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "bulk_semantics_users", type: :table},
        select_sql: "SELECT 1::INTEGER AS id, 'seed'::VARCHAR AS name",
        options: %{appender_rows: [[2, "alpha"]]}
      }

    assert {:ok, %Result{kind: :materialize, command: "appender", rows_affected: 1}} =
             SQL.materialize(session, plan)

    assert {:error, %Error{type: :execution_error, operation: :materialize}} =
             SQL.materialize(session, plan)

    assert {:ok, %Result{rows: [%{"id" => 2, "name" => "alpha"}]}} =
             SQL.query(session, "SELECT id, name FROM bulk_semantics_users")
  end

  test "appender failure path cleans up for later replacement", %{session: session} do
    failing_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "bulk_retry_users", type: :table},
        select_sql: "SELECT 1::INTEGER AS id, 'seed'::VARCHAR AS name",
        options: %{appender_rows: [[2]]}
      }

    assert {:error, %Error{type: :execution_error, operation: :materialize}} =
             SQL.materialize(session, failing_plan)

    retry_plan =
      %WritePlan{
        failing_plan
        | replace?: true,
          options: %{appender_rows: [[4, "delta"]]}
      }

    assert {:ok, %Result{kind: :materialize, command: "appender", rows_affected: 1}} =
             SQL.materialize(session, retry_plan)

    assert {:ok, %Result{rows: [%{"id" => 4, "name" => "delta"}]}} =
             SQL.query(session, "SELECT id, name FROM bulk_retry_users")
  end

  test "materialize view via fallback sql generation", %{session: session} do
    assert {:ok, %Result{}} = SQL.execute(session, "CREATE TABLE src_orders(id INTEGER)")
    assert {:ok, %Result{}} = SQL.execute(session, "INSERT INTO src_orders VALUES (10), (11)")

    plan =
      %WritePlan{
        materialization: :view,
        target: %Relation{schema: "main", name: "vw_src_orders", type: :view},
        select_sql: "SELECT id FROM src_orders",
        replace?: true
      }

    assert {:ok, %Result{kind: :materialize}} = SQL.materialize(session, plan)

    assert {:ok, %Result{rows: [%{"id" => 10}, %{"id" => 11}]}} =
             SQL.query(session, "SELECT id FROM vw_src_orders ORDER BY id")
  end
end
