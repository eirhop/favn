defmodule FavnDuckdbADBC.SQLAdapterDuckDBADBCTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Adapter.DuckDB.ADBC.Client.ADBC, as: ADBCClient
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Error
  alias Favn.SQL.Relation
  alias Favn.SQL.WritePlan
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

      case TestSupport.mode(:query_mode, :ok) do
        :metadata_capacity -> {:error, "DuckLake metadata capacity exceeded; retry later"}
        :ok -> {:ok, result_ref}
      end
    end

    @impl true
    def execute(_conn_ref, sql, params) do
      TestSupport.record({:execute, sql, params})

      case TestSupport.mode(:execute_mode, :ok) do
        :error -> {:error, :execute_failed}
        :ok -> {:ok, 2}
      end
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

    @impl true
    def rollback(conn_ref) do
      TestSupport.record({:rollback, conn_ref})

      case TestSupport.mode(:rollback_mode, :ok) do
        :ok -> :ok
        :no_active_transaction -> {:error, "TransactionContext Error: no active transaction"}
        :error -> {:error, :rollback_failed}
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
    TestSupport.put_mode(:query_mode, :ok)
    TestSupport.put_mode(:execute_mode, :ok)
    TestSupport.put_mode(:columns_mode, :ok)
    TestSupport.put_mode(:fetch_mode, :ok)
    TestSupport.put_mode(:rollback_mode, :ok)

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

  test "pool lifecycle hooks validate, reset rollback, and restore configured USE" do
    resolved = %Resolved{
      resolved()
      | config: %{
          open: [database: ":memory:"],
          duckdb: [attach: [lake: [type: :ducklake]], use: :lake]
        }
    }

    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert ADBC.poolable?(resolved, [])

    assert %{adapter: ADBC, client: FakeClient, driver: "/opt/duckdb/libduckdb.so"} =
             ADBC.pool_fingerprint(resolved,
               duckdb_adbc_client: FakeClient,
               duckdb_adbc: [driver: "/opt/duckdb/libduckdb.so"]
             )

    assert :ok = ADBC.validate_session(conn, [])
    assert :ok = ADBC.reset_session(conn, resolved, required_catalogs: [:lake])

    assert Enum.any?(events(), fn
             {:rollback, _conn_ref} -> true
             _event -> false
           end)

    assert Enum.any?(events(), fn
             {:execute, "USE \"lake\"", []} -> true
             _event -> false
           end)
  end

  test "pool reset tolerates no-active-transaction rollback and skips USE outside required catalogs" do
    TestSupport.put_mode(:rollback_mode, :no_active_transaction)

    resolved = %Resolved{
      resolved()
      | config: %{
          open: [database: ":memory:"],
          duckdb: [attach: [lake: [type: :ducklake]], use: :lake]
        }
    }

    {:ok, conn} = ADBC.connect(resolved, duckdb_adbc_client: FakeClient)

    assert :ok = ADBC.reset_session(conn, resolved, required_catalogs: [:main])

    refute Enum.any?(events(), fn
             {:execute, "USE \"lake\"", []} -> true
             _event -> false
           end)
  end

  test "metadata capacity errors classify as retryable capacity" do
    TestSupport.put_mode(:query_mode, :metadata_capacity)
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert {:error,
            %Error{
              retryable?: true,
              details: %{classification: :capacity}
            }} = ADBC.query(conn, "SELECT 1", [])

    assert %{classification: :capacity, retryable?: true, capacity?: true} =
             ADBC.classify_error("DuckLake metadata capacity exceeded; retry later", [])
  end

  test "ducklake storage stays conservative by default" do
    resolved = %Resolved{
      resolved()
      | config: %{open: [database: "/tmp/favn-adbc.duckdb"], duckdb_storage: :ducklake}
    }

    assert %ConcurrencyPolicy{limit: 1, applies_to: :all, scope: {:duckdb_adbc_database, _path}} =
             ADBC.default_concurrency_policy(resolved)
  end

  test "ducklake catalogs on the same postgres metadata server share admission scope" do
    resolved = %Resolved{
      resolved()
      | config: %{
          open: [database: ":memory:"],
          duckdb: [
            secrets: [
              raw_meta: [type: :postgres, host: "pg.example.com", port: 5432],
              mart_meta: [type: :postgres, host: "pg.example.com", port: 5432]
            ],
            attach: [
              raw: [type: :ducklake, meta_secret: :raw_meta, write_concurrency: 1],
              mart: [type: :ducklake, meta_secret: :mart_meta, write_concurrency: 10]
            ]
          ]
        }
    }

    assert {:ok, policies} = ADBC.concurrency_policies(resolved)

    scopes =
      policies
      |> Enum.filter(&match?(%ConcurrencyPolicy{target: {:catalog, _}}, &1))
      |> Enum.map(& &1.scope)

    assert length(scopes) == 2
    assert scopes |> Enum.uniq() |> length() == 1
    assert [%ConcurrencyPolicy{limit: 1}, %ConcurrencyPolicy{limit: 1}] = Enum.drop(policies, 1)

    assert {:ok, other_connection_policies} =
             ADBC.concurrency_policies(%Resolved{resolved | name: :other_warehouse})

    other_scope =
      other_connection_policies
      |> Enum.find(&match?(%ConcurrencyPolicy{target: {:catalog, "raw"}}, &1))
      |> Map.fetch!(:scope)

    assert other_scope == hd(scopes)
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

  test "catalog-qualified materialization statements include schema setup and target" do
    plan = %WritePlan{
      materialization: :table,
      target: %Relation{catalog: "raw", schema: "sales", name: "products", type: :table},
      select_sql: "SELECT 1 AS id",
      replace_existing?: true
    }

    assert {:ok, statements} = ADBC.materialization_statements(plan, %Capabilities{}, [])
    sql = Enum.map(statements, &IO.iodata_to_binary/1)

    assert Enum.any?(sql, &(&1 == ~s(CREATE SCHEMA IF NOT EXISTS "raw"."sales")))

    assert Enum.any?(
             sql,
             &String.starts_with?(&1, ~s(CREATE OR REPLACE TABLE "raw"."sales"."products"))
           )
  end

  test "catalog-qualified materialization statements reject missing schema" do
    plan = %WritePlan{
      materialization: :table,
      target: %Relation{catalog: "raw", name: "products", type: :table},
      select_sql: "SELECT 1 AS id",
      replace_existing?: true
    }

    assert {:error, %Error{type: :execution_error}} =
             ADBC.materialization_statements(plan, %Capabilities{}, [])
  end

  test "catalog-qualified bulk insert rows return unsupported capability" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    plan = %WritePlan{
      materialization: :table,
      target: %Relation{catalog: "raw", schema: "sales", name: "bulk_products", type: :table},
      select_sql: "SELECT 1 AS id",
      replace_existing?: true,
      options: %{appender_rows: [%{"id" => 1}]}
    }

    assert {:error,
            %Error{
              type: :unsupported_capability,
              operation: :materialize,
              message:
                "DuckDB ADBC bulk insert materialization does not support catalog-qualified targets"
            }} = ADBC.materialize(conn, plan, [])
  end

  test "catalog-qualified relation and columns introspection filter catalog and schema" do
    ref = %RelationRef{catalog: "raw", schema: "sales", name: "products"}

    assert {:ok, relation_query} = ADBC.introspection_query(:relation, ref, [])
    relation_sql = IO.iodata_to_binary(relation_query)

    assert relation_sql =~ "table_schema = 'sales'"
    assert relation_sql =~ "table_catalog = 'raw'"
    assert relation_sql =~ "table_name = 'products'"

    assert {:ok, columns_query} = ADBC.introspection_query(:columns, ref, [])
    columns_sql = IO.iodata_to_binary(columns_query)

    assert columns_sql =~ "table_schema = 'sales'"
    assert columns_sql =~ "table_catalog = 'raw'"
    assert columns_sql =~ "table_name = 'products'"

    assert {:ok, list_query} = ADBC.introspection_query(:list_relations, ref, [])
    list_sql = IO.iodata_to_binary(list_query)

    assert list_sql =~ "table_schema = 'sales'"
    assert list_sql =~ "table_catalog = 'raw'"
  end

  test "catalog-qualified introspection rejects missing schema" do
    ref = %RelationRef{catalog: "raw", name: "products"}

    assert_raise ArgumentError, ~r/catalog-qualified relations require schema/, fn ->
      ADBC.introspection_query(:relation, ref, [])
    end
  end

  test "row_count rejects catalog-qualified relation without schema" do
    {:ok, conn} = ADBC.connect(resolved(), duckdb_adbc_client: FakeClient)

    assert_raise ArgumentError, ~r/catalog-qualified relations require schema/, fn ->
      ADBC.row_count(conn, %RelationRef{catalog: "raw", name: "orders"}, [])
    end
  end

  test "production local-file storage rejects memory database before opening DuckDB" do
    resolved = %Resolved{resolved() | config: %{open: [database: ":memory:"], production?: true}}

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
      config: %{open: [database: ":memory:"]}
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
