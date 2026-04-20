defmodule FavnDuckdb.SQLAdapterDuckDBHardeningTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.Error
  alias Favn.SQL.Relation
  alias Favn.SQL.WritePlan

  @events_table :favn_duckdb_adapter_hardening_events

  defmodule FakeClient do
    @behaviour Favn.SQL.Adapter.DuckDB.Client

    @events_table :favn_duckdb_adapter_hardening_events

    @impl true
    def open(_database) do
      db_ref = make_ref()
      record({:open, db_ref})
      {:ok, db_ref}
    end

    @impl true
    def connection(db_ref) do
      record({:connection, db_ref})

      case mode(:connection_mode, :ok) do
        :ok -> {:ok, make_ref()}
        :error -> {:error, :connection_failed}
      end
    end

    @impl true
    def query(conn_ref, sql, _params) do
      record({:query, conn_ref, sql})

      case {mode(:query_mode, :ok), String.starts_with?(sql, "INSERT")} do
        {:error, true} ->
          {:error, :write_failed}

        {:conflict, true} ->
          {:error, "Transaction conflict: cannot update a table that has been altered!"}

        _ ->
          result_ref = make_ref()
          record({:result_ref, result_ref, sql})
          {:ok, result_ref}
      end
    end

    @impl true
    def fetch_all(result_ref) do
      record({:fetch_all, result_ref})

      case mode(:fetch_mode, :ok) do
        :ok -> [[1]]
        :error -> :fetch_error
        :raise -> raise "fetch failed"
      end
    end

    @impl true
    def columns(result_ref) do
      record({:columns, result_ref})
      ["value"]
    end

    @impl true
    def begin_transaction(conn_ref) do
      record({:begin_transaction, conn_ref})

      case mode(:begin_mode, :ok) do
        :ok -> :ok
        :error -> {:error, "begin failed"}
      end
    end

    @impl true
    def commit(conn_ref) do
      record({:commit, conn_ref})

      case mode(:commit_mode, :ok) do
        :ok -> :ok
        :error -> {:error, "commit failed"}
      end
    end

    @impl true
    def rollback(conn_ref) do
      record({:rollback, conn_ref})

      case mode(:rollback_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :rollback_failed}
      end
    end

    @impl true
    def appender(conn_ref, _table_name, _schema) do
      appender_ref = make_ref()
      record({:appender_open, conn_ref, appender_ref})
      {:ok, appender_ref}
    end

    @impl true
    def appender_add_rows(appender_ref, _rows) do
      record({:appender_add_rows, appender_ref})

      case mode(:appender_add_rows_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_add_rows_failed}
      end
    end

    @impl true
    def appender_flush(appender_ref) do
      record({:appender_flush, appender_ref})

      case mode(:appender_flush_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_flush_failed}
      end
    end

    @impl true
    def appender_close(appender_ref) do
      record({:appender_close, appender_ref})

      case mode(:appender_close_mode, :ok) do
        :ok -> :ok
        :error -> {:error, :appender_close_failed}
      end
    end

    @impl true
    def release(resource) do
      record({:release, resource})
      :ok
    end

    defp mode(key, default), do: Application.get_env(:favn, key, default)

    defp record(event) do
      if :ets.whereis(@events_table) != :undefined do
        :ets.insert(@events_table, {System.unique_integer([:positive]), event})
      end

      :ok
    end
  end

  setup do
    if :ets.whereis(@events_table) != :undefined do
      :ets.delete(@events_table)
    end

    :ets.new(@events_table, [:named_table, :ordered_set, :public])

    keys = [
      :connection_mode,
      :query_mode,
      :fetch_mode,
      :begin_mode,
      :commit_mode,
      :rollback_mode,
      :appender_add_rows_mode,
      :appender_flush_mode,
      :appender_close_mode
    ]

    Enum.each(keys, &Application.put_env(:favn, &1, :ok))

    on_exit(fn ->
      Enum.each(keys, &Application.delete_env(:favn, &1))

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ok
  end

  test "begin failure keeps begin-stage transaction error and does not rollback" do
    Application.put_env(:favn, :begin_mode, :error)
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
    Application.put_env(:favn, :commit_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{operation: :transaction, details: %{transaction_stage: :commit}}} =
             DuckDB.transaction(conn, fn _ -> {:ok, :ok} end, [])

    assert Enum.any?(events(), fn
             {:rollback, _conn_ref} -> true
             _ -> false
           end)
  end

  test "query fetch error releases result handle" do
    Application.put_env(:favn, :fetch_mode, :error)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{type: :execution_error, operation: :query}} =
             DuckDB.query(conn, "SELECT 1", [])

    {result_ref, _sql} = last_result_ref!()

    assert Enum.any?(events(), fn
             {:release, ^result_ref} -> true
             _ -> false
           end)
  end

  test "releases database handle when connection allocation fails" do
    Application.put_env(:favn, :connection_mode, :error)

    assert {:error, %Error{type: :connection_error, operation: :connect}} =
             DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert Enum.any?(events(), fn
             {:release, _resource} -> true
             _ -> false
           end)
  end

  test "appender failure still cleans up appender deterministically" do
    Application.put_env(:favn, :appender_add_rows_mode, :error)
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

  test "conflict failures normalize as retryable" do
    Application.put_env(:favn, :query_mode, :conflict)
    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error,
            %Error{operation: :execute, retryable?: true, details: %{classification: :conflict}}} =
             DuckDB.execute(conn, "INSERT INTO t VALUES (1)", [])
  end

  defp resolved do
    %Resolved{
      name: :duckdb_runtime,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{database: ":memory:"}
    }
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
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
