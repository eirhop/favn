defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Error
  alias FavnDuckdbADBC.TestSupport

  defmodule FakeClient do
    use FavnDuckdbADBC.TestSupport.FakeClient

    alias FavnDuckdbADBC.TestSupport

    @impl true
    def open(database, opts) do
      TestSupport.record({:open, database, opts})

      case TestSupport.mode(:open_mode, :ok) do
        :ok -> {:ok, make_ref()}
        :error -> {:error, :open_failed}
      end
    end

    @impl true
    def connection(db_ref) do
      TestSupport.record({:connection, db_ref})
      {:ok, make_ref()}
    end

    @impl true
    def query(_conn_ref, sql, params) do
      result_ref = make_ref()
      TestSupport.record({:query, result_ref, sql, params})
      {:ok, result_ref}
    end

    @impl true
    def execute(_conn_ref, sql, params) do
      TestSupport.record({:execute, sql, params})
      {:ok, 2}
    end

    @impl true
    def columns(result_ref) do
      TestSupport.record({:columns, result_ref})

      case TestSupport.mode(:columns_mode, :ok) do
        :ok -> ["value"]
        :error -> {:error, :columns_failed}
      end
    end

    @impl true
    def fetch_all(result_ref, max_rows) do
      TestSupport.record({:fetch_all, result_ref, max_rows})

      case TestSupport.mode(:fetch_mode, :ok) do
        :ok -> [%{value: 1}]
        :limit -> {:error, {:result_row_limit_exceeded, 3, max_rows}}
      end
    end

    @impl true
    def release(resource) do
      TestSupport.record({:release, resource})
      :ok
    end
  end

  setup do
    TestSupport.start_events()
    TestSupport.put_mode(:open_mode, :ok)
    TestSupport.put_mode(:columns_mode, :ok)
    TestSupport.put_mode(:fetch_mode, :ok)

    on_exit(fn -> TestSupport.reset() end)

    :ok
  end

  test "query returns normalized rows, columns, and releases result" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient, max_rows: 5)

    assert {:ok, result} = ADBC.query(conn, "SELECT 1", [])
    assert result.rows == [%{"value" => 1}]
    assert result.columns == ["value"]
    assert result.metadata.row_limit == 5

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _event -> false
           end)
  end

  test "bounded query errors are normalized and release result" do
    TestSupport.put_mode(:fetch_mode, :limit)
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient, max_rows: 2)

    assert {:error,
            %Error{
              type: :execution_error,
              operation: :query,
              details: %{classification: :bounded_result}
            }} = ADBC.query(conn, "SELECT * FROM large_table", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _event -> false
           end)
  end

  test "execute uses ADBC execute path without fetching rows" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert {:ok, result} = ADBC.execute(conn, "CREATE TABLE t AS SELECT 1", [])
    assert result.kind == :execute
    assert result.rows_affected == 2

    refute Enum.any?(events(), fn
             {:fetch_all, _result_ref, _limit} -> true
             _event -> false
           end)
  end

  test "row_count uses adapter-owned quoted relation SQL" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert {:ok, 0} = ADBC.row_count(conn, %RelationRef{schema: "raw data", name: "orders"}, [])

    assert Enum.any?(events(), fn
             {:query, _result_ref, "SELECT count(*) AS row_count FROM \"raw data\".\"orders\"", []} ->
               true

             _event ->
               false
           end)
  end

  test "production local-file storage rejects memory database before opening DuckDB" do
    resolved = %Resolved{resolved() | config: %{database: ":memory:", production?: true}}

    assert {:error, %Error{type: :invalid_config, operation: :connect, details: %{reason: :memory_database}}} =
             ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    refute Enum.any?(events(), fn
             {:open, _database, _opts} -> true
             _event -> false
           end)
  end

  defp resolved do
    %Resolved{name: :warehouse, adapter: ADBC, module: __MODULE__, config: %{database: ":memory:"}}
  end

  defp last_result_ref! do
    events()
    |> Enum.find_value(fn
      {:query, result_ref, sql, _params} -> {result_ref, sql}
      _event -> nil
    end)
  end

  defp events, do: TestSupport.events()
end
