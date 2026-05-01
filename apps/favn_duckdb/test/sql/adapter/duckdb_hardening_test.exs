defmodule FavnDuckdb.SQLAdapterDuckDBHardeningTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.RelationRef
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Client
  alias Favn.SQL.ConcurrencyPolicy
  alias Favn.SQL.Error
  alias Favn.SQL.Relation
  alias Favn.SQL.Session
  alias Favn.SQL.WritePlan
  alias FavnDuckdb.TestSupport

  defmodule FakeClient do
    use FavnDuckdb.TestSupport.FakeClient

    alias FavnDuckdb.TestSupport

    @impl true
    def open(database) do
      db_ref = make_ref()
      TestSupport.record({:open, db_ref, database})

      case TestSupport.mode(:open_mode, :ok) do
        :ok -> {:ok, db_ref}
        :worker_not_available -> {:error, :worker_not_available}
      end
    end

    @impl true
    def connection(db_ref) do
      TestSupport.record({:connection, db_ref})

      case TestSupport.mode(:connection_mode, :ok) do
        :ok -> {:ok, make_ref()}
        :error -> {:error, :connection_failed}
      end
    end

    @impl true
    def query(conn_ref, sql, params) do
      TestSupport.record({:query, conn_ref, sql})
      TestSupport.record({:query_params, sql, params})

      case {TestSupport.mode(:query_mode, :ok), String.starts_with?(sql, "INSERT")} do
        {:worker_timeout, _write?} ->
          {:error, :worker_call_timeout}

        {:error, true} ->
          {:error, :write_failed}

        {:conflict, true} ->
          {:error, "Transaction conflict: cannot update a table that has been altered!"}

        _ ->
          result_ref = make_ref()
          TestSupport.record({:result_ref, result_ref, sql})
          {:ok, result_ref}
      end
    end

    @impl true
    def fetch_all(result_ref) do
      TestSupport.record({:fetch_all, result_ref})

      case TestSupport.mode(:fetch_mode, :ok) do
        :ok -> [[1]]
        :error -> :fetch_error
        :raise -> raise "fetch failed"
      end
    end

    @impl true
    def columns(result_ref) do
      TestSupport.record({:columns, result_ref})

      case TestSupport.mode(:columns_mode, :ok) do
        :ok ->
          sql = result_sql(result_ref)

          if is_binary(sql) and String.contains?(sql, "count(*) AS row_count") do
            ["row_count"]
          else
            ["value"]
          end

        :error ->
          {:error, :columns_failed}
      end
    end

    defp result_sql(result_ref) do
      Enum.find_value(TestSupport.events(), fn
        {:result_ref, ^result_ref, sql} -> sql
        _other -> nil
      end)
    end

    @impl true
    def begin_transaction(conn_ref) do
      TestSupport.record({:begin_transaction, conn_ref})

      case TestSupport.mode(:begin_mode, :ok) do
        :ok -> :ok
        :error -> {:error, "begin failed"}
      end
    end

    @impl true
    def commit(conn_ref) do
      TestSupport.record({:commit, conn_ref})

      case TestSupport.mode(:commit_mode, :ok) do
        :ok -> :ok
        :error -> {:error, "commit failed"}
      end
    end

    @impl true
    def rollback(conn_ref) do
      TestSupport.record({:rollback, conn_ref})

      case TestSupport.mode(:rollback_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :rollback_failed}
      end
    end

    @impl true
    def appender(conn_ref, _table_name, _schema) do
      appender_ref = make_ref()
      TestSupport.record({:appender_open, conn_ref, appender_ref})
      {:ok, appender_ref}
    end

    @impl true
    def appender_add_rows(appender_ref, _rows) do
      TestSupport.record({:appender_add_rows, appender_ref})

      case TestSupport.mode(:appender_add_rows_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_add_rows_failed}
      end
    end

    @impl true
    def appender_flush(appender_ref) do
      TestSupport.record({:appender_flush, appender_ref})

      case TestSupport.mode(:appender_flush_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_flush_failed}
      end
    end

    @impl true
    def appender_close(appender_ref) do
      TestSupport.record({:appender_close, appender_ref})

      case TestSupport.mode(:appender_close_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_close_failed}
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

    keys = [
      :open_mode,
      :connection_mode,
      :query_mode,
      :fetch_mode,
      :columns_mode,
      :begin_mode,
      :commit_mode,
      :rollback_mode,
      :appender_add_rows_mode,
      :appender_flush_mode,
      :appender_close_mode
    ]

    TestSupport.put_modes(keys, :ok)

    on_exit(fn ->
      TestSupport.reset()
    end)

    :ok
  end

  test "begin failure keeps begin-stage transaction error and does not rollback" do
    TestSupport.put_mode(:begin_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{operation: :transaction, details: %{transaction_stage: :begin}}} =
             DuckDB.transaction(conn, fn _ -> {:ok, :ok} end, [])

    refute Enum.any?(events(), fn
             {:rollback, _conn_ref} -> true
             _ -> false
           end)
  end

  test "transaction body raise triggers rollback" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{operation: :transaction, message: "transaction body raised exception"}} =
             DuckDB.transaction(conn, fn _ -> raise "boom" end, [])

    assert Enum.any?(events(), fn
             {:rollback, _conn_ref} -> true
             _ -> false
           end)
  end

  test "commit failure preserves commit-stage error and rolls back" do
    TestSupport.put_mode(:commit_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{operation: :transaction, details: %{transaction_stage: :commit}}} =
             DuckDB.transaction(conn, fn _ -> {:ok, :ok} end, [])

    assert Enum.any?(events(), fn
             {:rollback, _conn_ref} -> true
             _ -> false
           end)
  end

  test "rollback failure keeps original transaction failure context" do
    TestSupport.put_mode(:rollback_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error,
            %Error{
              operation: :transaction,
              message: "transaction rollback failed",
              retryable?: false,
              details: %{transaction_stage: :rollback, original_error: %{message: "body failed"}}
            }} = DuckDB.transaction(conn, fn _ -> {:error, "body failed"} end, [])
  end

  test "query success releases result handle" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)
    assert {:ok, _result} = DuckDB.query(conn, "SELECT 1", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _ -> false
           end)
  end

  test "row_count uses adapter-owned quoted relation SQL" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:ok, 1} =
             DuckDB.row_count(conn, %RelationRef{schema: "raw data", name: "orders"}, [])

    assert Enum.any?(events(), fn
             {:query, _conn_ref, "SELECT count(*) AS row_count FROM \"raw data\".\"orders\""} ->
               true

             _ ->
               false
           end)
  end

  test "sample caps limits and uses adapter-owned quoted relation SQL" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:ok, result} =
             DuckDB.sample(conn, %RelationRef{schema: "raw", name: "orders"}, limit: 100)

    assert result.rows == [%{"value" => 1}]

    assert Enum.any?(events(), fn
             {:query, _conn_ref, "SELECT * FROM \"raw\".\"orders\" LIMIT 20"} -> true
             _ -> false
           end)
  end

  test "query fetch error releases result handle" do
    TestSupport.put_mode(:fetch_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{type: :execution_error, operation: :query}} =
             DuckDB.query(conn, "SELECT 1", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _ -> false
           end)
  end

  test "query columns error releases result handle" do
    TestSupport.put_mode(:columns_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{type: :execution_error, operation: :query}} =
             DuckDB.query(conn, "SELECT 1", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _ -> false
           end)

    refute Enum.any?(events(), fn
             {:fetch_all, ^result_ref} -> true
             _ -> false
           end)
  end

  test "query fetch raise releases result handle" do
    TestSupport.put_mode(:fetch_mode, :raise)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert_raise RuntimeError, "fetch failed", fn ->
      DuckDB.query(conn, "SELECT 1", [])
    end

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _ -> false
           end)
  end

  test "releases database handle when connection allocation fails" do
    TestSupport.put_mode(:connection_mode, :error)

    assert {:error, %Error{type: :connection_error, operation: :connect}} =
             DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert Enum.any?(events(), fn
             {:release, _resource} -> true
             _ -> false
           end)
  end

  test "worker unavailable connect failure has actionable retryable diagnostics" do
    TestSupport.put_mode(:open_mode, :worker_not_available)

    assert {:error,
            %Error{
              type: :connection_error,
              operation: :connect,
              message: "DuckDB worker is not available",
              retryable?: true,
              details: %{classification: :worker_unavailable, reason: ":worker_not_available"}
            }} = DuckDB.connect(resolved(), duckdb_client: FakeClient)
  end

  test "worker timeout execution failure has actionable retryable diagnostics" do
    TestSupport.put_mode(:query_mode, :worker_timeout)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error,
            %Error{
              type: :execution_error,
              operation: :query,
              message: "DuckDB worker call timed out; operation outcome is unknown",
              retryable?: false,
              details: %{
                classification: :unknown_outcome_timeout,
                reason: ":worker_call_timeout"
              }
            }} = DuckDB.query(conn, "SELECT 1", [])
  end

  test "production local-file storage rejects missing database before opening DuckDB" do
    resolved = production_resolved(%{database: nil})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :missing_database}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects memory database before opening DuckDB" do
    resolved = production_resolved(%{database: ":memory:"})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :memory_database}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects relative database before opening DuckDB" do
    resolved = production_resolved(%{database: "tmp/tutorial.duckdb"})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :relative_database}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects blank database before opening DuckDB" do
    resolved = production_resolved(%{database: "  "})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :blank_database}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects whitespace-padded absolute database before opening DuckDB" do
    path = " " <> tmp_duckdb_path("production_padded") <> " "
    resolved = production_resolved(%{database: path})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :invalid_database}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production storage rejects unknown storage mode before opening DuckDB" do
    resolved = production_resolved(%{database: ":memory:", duckdb_storage: :unknown})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :invalid_storage_mode}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects missing parent directory before opening DuckDB" do
    path = Path.join([System.tmp_dir!(), "favn_missing_parent", "warehouse.duckdb"])
    resolved = production_resolved(%{database: path})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :missing_parent_directory}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage rejects unwritable parent directory before opening DuckDB" do
    parent = Path.join(System.tmp_dir!(), "favn_unwritable_#{System.unique_integer([:positive])}")
    File.mkdir!(parent)
    File.chmod!(parent, 0o555)

    on_exit(fn ->
      File.chmod(parent, 0o755)
      File.rmdir(parent)
    end)

    resolved = production_resolved(%{database: Path.join(parent, "warehouse.duckdb")})

    assert {:error,
            %Error{
              type: :invalid_config,
              operation: :connect,
              details: %{reason: :unwritable_parent_directory}
            }} = DuckDB.connect(resolved, duckdb_client: FakeClient)

    refute opened_duckdb?()
  end

  test "production local-file storage accepts absolute database with writable parent" do
    path = tmp_duckdb_path("production_valid")
    resolved = production_resolved(%{database: path})

    assert {:ok, _conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)
    assert opened_duckdb?(path)
  end

  test "production external storage allows memory database" do
    resolved = production_resolved(%{database: ":memory:", duckdb_storage: :external})

    assert {:ok, _conn} = DuckDB.connect(resolved, duckdb_client: FakeClient)
    assert opened_duckdb?(":memory:")
  end

  test "non-production local behavior still allows memory database" do
    assert {:ok, _conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)
    assert opened_duckdb?(":memory:")
  end

  test "production storage schema fields expose DuckDB-owned config keys" do
    assert [production_field, storage_field] = DuckDB.production_storage_schema_fields()

    assert production_field == %{key: :production?, type: :boolean, default: false}

    assert storage_field == %{
             key: :duckdb_storage,
             type: {:in, [:local_file, :external, :ephemeral, :ducklake]},
             default: :local_file
           }
  end

  test "appender failure still cleans up appender deterministically" do
    TestSupport.put_mode(:appender_add_rows_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    write_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "bulk_users_fail", type: :table},
        select_sql: "SELECT 1 AS id",
        options: %{appender_rows: [[1], [2]]}
      }

    assert {:error, %Error{operation: :materialize}} = DuckDB.materialize(conn, write_plan, [])

    appender_ref =
      events()
      |> Enum.find_value(fn
        {:appender_open, _conn_ref, ref} -> ref
        _ -> nil
      end)

    assert appender_ref

    assert 1 ==
             Enum.count(events(), fn
               {:appender_close, ^appender_ref} -> true
               _ -> false
             end)
  end

  test "appender success closes appender without extra release" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    write_plan =
      %WritePlan{
        materialization: :table,
        target: %Relation{schema: "main", name: "bulk_users", type: :table},
        select_sql: "SELECT 1 AS id",
        options: %{appender_rows: [[1], [2]]}
      }

    assert {:ok, %Favn.SQL.Result{kind: :materialize, command: "appender"}} =
             DuckDB.materialize(conn, write_plan, [])

    appender_ref =
      events()
      |> Enum.find_value(fn
        {:appender_open, _conn_ref, ref} -> ref
        _ -> nil
      end)

    assert appender_ref

    assert 1 ==
             Enum.count(events(), fn
               {:appender_close, ^appender_ref} -> true
               _ -> false
             end)

    refute Enum.any?(events(), fn
             {:release, ^appender_ref} -> true
             _ -> false
           end)
  end

  test "conflict failures normalize as retryable" do
    TestSupport.put_mode(:query_mode, :conflict)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error,
            %Error{operation: :execute, retryable?: true, details: %{classification: :conflict}}} =
             DuckDB.execute(conn, "INSERT INTO t VALUES (1)", [])
  end

  test "local file databases default to single admitted SQL operation" do
    resolved = %Resolved{resolved() | config: %{database: "tmp/tutorial.duckdb"}}

    assert %ConcurrencyPolicy{
             limit: 1,
             scope: {:duckdb_database, path},
             applies_to: :all
           } = DuckDB.default_concurrency_policy(resolved)

    assert path == Path.expand("tmp/tutorial.duckdb")
  end

  test "DuckLake mode defaults to unlimited SQL write concurrency" do
    resolved = %Resolved{resolved() | config: %{database: "tmp/tutorial.duckdb", mode: :ducklake}}

    assert %ConcurrencyPolicy{limit: :unlimited, applies_to: :writes} =
             DuckDB.default_concurrency_policy(resolved)
  end

  test "same-file materializations through SQL client are admitted serially" do
    path =
      Path.join(
        System.tmp_dir!(),
        "favn_duckdb_admission_#{System.unique_integer([:positive])}.duckdb"
      )

    resolved = %Resolved{resolved() | config: %{database: path}}

    {:ok, session_a} = open_session(resolved)
    {:ok, session_b} = open_session(resolved)

    on_exit(fn ->
      Client.disconnect(session_a)
      Client.disconnect(session_b)
      File.rm(path)
    end)

    tasks = [
      Task.async(fn -> Client.materialize(session_a, table_plan("concurrent_a", 1), []) end),
      Task.async(fn -> Client.materialize(session_b, table_plan("concurrent_b", 2), []) end)
    ]

    assert Enum.all?(Task.await_many(tasks, 5_000), &match?({:ok, %Favn.SQL.Result{}}, &1))

    assert {:ok, %Relation{name: "concurrent_a"}} =
             Client.relation(session_a, RelationRef.new!(schema: "main", name: "concurrent_a"))

    assert {:ok, %Relation{name: "concurrent_b"}} =
             Client.relation(session_b, RelationRef.new!(schema: "main", name: "concurrent_b"))
  end

  test "table materialization creates missing target schema" do
    path = tmp_duckdb_path("schema_table")
    resolved = %Resolved{resolved() | config: %{database: path}}
    {:ok, session} = open_session(resolved)

    on_exit(fn ->
      Client.disconnect(session)
      File.rm(path)
    end)

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} =
             Client.materialize(session, table_plan("analytics", "daily_revenue", 1), [])

    assert {:ok, %Relation{schema: "analytics", name: "daily_revenue", type: :table}} =
             Client.relation(
               session,
               RelationRef.new!(schema: "analytics", name: "daily_revenue")
             )
  end

  test "table materialization creates missing target schema without passing select params to schema setup" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    plan = %WritePlan{
      materialization: :table,
      target: %Relation{schema: "analytics", name: "parameterized_daily_revenue", type: :table},
      select_sql: "SELECT ? AS id",
      replace_existing?: true
    }

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} =
             DuckDB.materialize(conn, plan, params: [42])

    assert_schema_setup_has_no_params(~s(CREATE SCHEMA IF NOT EXISTS "analytics"))

    assert_statement_has_params(
      ~s(CREATE OR REPLACE TABLE "analytics"."parameterized_daily_revenue"),
      [42]
    )
  end

  test "view materialization creates missing target schema" do
    path = tmp_duckdb_path("schema_view")
    resolved = %Resolved{resolved() | config: %{database: path}}
    {:ok, session} = open_session(resolved)

    on_exit(fn ->
      Client.disconnect(session)
      File.rm(path)
    end)

    plan = %WritePlan{
      materialization: :view,
      target: %Relation{schema: "mart", name: "active_customers", type: :view},
      select_sql: "SELECT 1 AS id",
      replace_existing?: true
    }

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} = Client.materialize(session, plan, [])

    assert {:ok, %Relation{schema: "mart", name: "active_customers", type: :view}} =
             Client.relation(session, RelationRef.new!(schema: "mart", name: "active_customers"))
  end

  test "view materialization creates missing target schema without passing select params to schema setup" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    plan = %WritePlan{
      materialization: :view,
      target: %Relation{schema: "mart", name: "parameterized_active_customers", type: :view},
      select_sql: "SELECT ? AS id",
      replace_existing?: true
    }

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} =
             DuckDB.materialize(conn, plan, params: [7])

    assert_schema_setup_has_no_params(~s(CREATE SCHEMA IF NOT EXISTS "mart"))

    assert_statement_has_params(
      ~s(CREATE OR REPLACE VIEW "mart"."parameterized_active_customers"),
      [7]
    )
  end

  test "incremental bootstrap creates missing target schema without passing select params to schema setup" do
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    plan = %WritePlan{
      materialization: :incremental,
      strategy: :append,
      mode: :bootstrap,
      target: %Relation{schema: "snapshots", name: "orders", type: :table},
      select_sql: "SELECT ? AS id",
      replace_existing?: true,
      bootstrap?: true
    }

    assert {:ok, %Favn.SQL.Result{kind: :materialize}} =
             DuckDB.materialize(conn, plan, params: [99])

    assert_schema_setup_has_no_params(~s(CREATE SCHEMA IF NOT EXISTS "snapshots"))
    assert_statement_has_params(~s(CREATE OR REPLACE TABLE "snapshots"."orders"), [99])
  end

  test "appender materialization creates missing target schema before opening appender" do
    path = tmp_duckdb_path("schema_appender")
    resolved = %Resolved{resolved() | config: %{database: path}}
    {:ok, session} = open_session(resolved)

    on_exit(fn ->
      Client.disconnect(session)
      File.rm(path)
    end)

    plan = %WritePlan{
      materialization: :table,
      target: %Relation{schema: "bulk", name: "events", type: :table},
      select_sql: "SELECT 1 AS id",
      replace_existing?: true,
      options: %{appender_rows: [[1], [2]]}
    }

    assert {:ok, %Favn.SQL.Result{kind: :materialize, command: "appender", rows_affected: 2}} =
             Client.materialize(session, plan, [])

    assert {:ok, %Relation{schema: "bulk", name: "events", type: :table}} =
             Client.relation(session, RelationRef.new!(schema: "bulk", name: "events"))
  end

  defp resolved do
    %Resolved{
      name: :duckdb_runtime,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{database: ":memory:"}
    }
  end

  defp production_resolved(config) do
    %Resolved{resolved() | config: Map.merge(%{production?: true}, config)}
  end

  defp open_session(%Resolved{} = resolved) do
    with {:ok, conn} <- DuckDB.connect(resolved, []),
         {:ok, capabilities} <- DuckDB.capabilities(resolved, []),
         {:ok, concurrency_policy} <- ConcurrencyPolicy.resolve(resolved) do
      {:ok,
       %Session{
         adapter: DuckDB,
         resolved: resolved,
         conn: conn,
         capabilities: capabilities,
         concurrency_policy: concurrency_policy
       }}
    end
  end

  defp table_plan(name, value) do
    table_plan("main", name, value)
  end

  defp table_plan(schema, name, value) do
    %WritePlan{
      materialization: :table,
      target: %Relation{schema: schema, name: name, type: :table},
      select_sql: "SELECT #{value} AS id",
      replace_existing?: true
    }
  end

  defp tmp_duckdb_path(name) do
    Path.join(
      System.tmp_dir!(),
      "favn_duckdb_#{name}_#{System.unique_integer([:positive])}.duckdb"
    )
  end

  defp last_result_ref! do
    events()
    |> Enum.reverse()
    |> Enum.find_value(fn
      {:result_ref, ref, sql} -> {ref, sql}
      _ -> nil
    end)
    |> case do
      nil -> flunk("expected at least one recorded result ref")
      value -> value
    end
  end

  defp events do
    TestSupport.events()
  end

  defp opened_duckdb? do
    Enum.any?(events(), fn
      {:open, _db_ref, _database} -> true
      _ -> false
    end)
  end

  defp opened_duckdb?(database) do
    Enum.any?(events(), fn
      {:open, _db_ref, ^database} -> true
      _ -> false
    end)
  end

  defp assert_schema_setup_has_no_params(statement_prefix) do
    assert Enum.any?(events(), fn
             {:query_params, statement, []} -> String.starts_with?(statement, statement_prefix)
             _ -> false
           end)
  end

  defp assert_statement_has_params(statement_prefix, params) do
    assert Enum.any?(events(), fn
             {:query_params, statement, ^params} ->
               String.starts_with?(statement, statement_prefix)

             _ ->
               false
           end)
  end
end
