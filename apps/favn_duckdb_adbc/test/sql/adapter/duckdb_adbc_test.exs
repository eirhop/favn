defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC, as: ADBCClient
  alias Favn.SQL.ConcurrencyPolicy
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
        :ok -> columns_for(result_ref)
        :error -> {:error, :columns_failed}
      end
    end

    defp columns_for(result_ref) do
      sql = result_sql(result_ref)

      if is_binary(sql) and String.contains?(sql, "SELECT version() AS duckdb_version") do
        ["duckdb_version"]
      else
        ["value"]
      end
    end

    @impl true
    def fetch_all(result_ref, max_rows, max_result_bytes, opts) do
      TestSupport.record({:fetch_all, result_ref, max_rows, max_result_bytes, opts})

      case TestSupport.mode(:fetch_mode, :ok) do
        :ok -> [row(result_ref)]
        :limit -> {:error, {:result_row_limit_exceeded, 3, max_rows}}
        :bytes -> {:error, {:result_byte_limit_exceeded, max_result_bytes + 1, max_result_bytes}}
      end
    end

    defp row(result_ref) do
      sql = result_sql(result_ref)

      if is_binary(sql) and String.contains?(sql, "SELECT version() AS duckdb_version") do
        %{duckdb_version: "v1.5.2"}
      else
        %{value: 1}
      end
    end

    defp result_sql(result_ref) do
      Enum.find_value(TestSupport.events(), fn
        {:query, ^result_ref, sql, []} -> sql
        _event -> nil
      end)
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
    {:ok, conn} =
      ADBC.connect(resolved(), duckdb_adbc_client: FakeClient, max_rows: 5, max_result_bytes: 100)

    assert {:ok, result} = ADBC.query(conn, "SELECT 1", [])
    assert result.rows == [%{"value" => 1}]
    assert result.columns == ["value"]
    assert result.metadata.row_limit == 5
    assert result.metadata.result_byte_limit == 100

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _event -> false
           end)
  end

  test "row-limit query errors are normalized and release result" do
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

  test "byte-limit query errors are normalized and release result" do
    TestSupport.put_mode(:fetch_mode, :bytes)
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient, max_result_bytes: 10)

    assert {:error,
            %Error{
              type: :execution_error,
              operation: :query,
              details: %{classification: :bounded_result}
            }} = ADBC.query(conn, "SELECT huge_json FROM large_table", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _event -> false
           end)
  end

  test "real ADBC client fails closed when result row count is unknown" do
    assert {:error, :result_row_count_unknown} =
             ADBCClient.fetch_all(%Adbc.Result{num_rows: nil, data: []}, 10, 1_000, [])
  end

  test "execute uses ADBC execute path without fetching rows" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert {:ok, result} = ADBC.execute(conn, "CREATE TABLE t AS SELECT 1", [])
    assert result.kind == :execute
    assert result.rows_affected == 2

    refute Enum.any?(events(), fn
             {:fetch_all, _result_ref, _row_limit, _byte_limit, _opts} -> true
             _event -> false
           end)
  end

  test "diagnostics opens driver, bootstraps, pings, and reports DuckDB version" do
    assert {:ok,
            %{
              status: :ok,
              preflight: %{
                connect?: true,
                ping?: true,
                bootstrap?: true,
                duckdb_version: "v1.5.2",
                driver: %{driver: :duckdb}
              }
            }} = ADBC.diagnostics(resolved(), duckdb_adbc_client: FakeClient)

    assert Enum.any?(events(), fn
             {:query, _result_ref, sql, []} when is_binary(sql) ->
               String.contains?(sql, "SELECT version() AS duckdb_version")

             _event ->
               false
           end)
  end

  test "diagnostics reports connection failures" do
    TestSupport.put_mode(:open_mode, :error)

    assert {:error,
            %{
              status: :unavailable,
              preflight: %{stage: :connect, error_type: :connection_error}
            }} = ADBC.diagnostics(resolved(), duckdb_adbc_client: FakeClient)
  end

  test "ducklake storage stays conservative by default" do
    resolved = %Resolved{
      resolved()
      | config: %{database: "/tmp/favn-adbc.duckdb", duckdb_storage: :ducklake}
    }

    assert %ConcurrencyPolicy{limit: 1, applies_to: :all, scope: {:duckdb_adbc_database, _path}} =
             ADBC.default_concurrency_policy(resolved)
  end

  test "row_count uses adapter-owned quoted relation SQL" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert {:ok, 0} = ADBC.row_count(conn, %RelationRef{schema: "raw data", name: "orders"}, [])

    assert Enum.any?(events(), fn
             {:query, _result_ref, sql, []} when is_binary(sql) ->
               String.contains?(sql, "SELECT count(*) AS row_count FROM \"raw data\".\"orders\"")

             _event ->
               false
           end)
  end

  test "production local-file storage rejects memory database before opening DuckDB" do
    resolved = %Resolved{resolved() | config: %{database: ":memory:", production?: true}}

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :memory_database}
            }} =
             ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    refute Enum.any?(events(), fn
             {:open, _database, _opts} -> true
             _event -> false
           end)
  end

  defp resolved do
    %Resolved{
      name: :warehouse,
      adapter: ADBC,
      module: __MODULE__,
      config: %{database: ":memory:"}
    }
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
