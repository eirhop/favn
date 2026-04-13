defmodule Favn.SQLDuckDBAdapterHardeningTest do
  use ExUnit.Case

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB
  alias Favn.SQL.{Error, Relation, WritePlan}

  @events_table :sql_duckdb_adapter_hardening_events

  defmodule FakeClient do
    @behaviour Favn.SQL.Adapter.DuckDB.Client

    @events_table :sql_duckdb_adapter_hardening_events

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

      if mode(:query_mode, :ok) == :error and String.starts_with?(sql, "INSERT") do
        {:error, :write_failed}
      else
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
      :ok
    end

    @impl true
    def commit(conn_ref) do
      record({:commit, conn_ref})
      :ok
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
    def appender(_conn_ref, _table_name, _schema), do: {:error, :appender_disabled}

    @impl true
    def appender_add_rows(_appender_ref, _rows), do: :ok

    @impl true
    def appender_flush(_appender_ref), do: :ok

    @impl true
    def appender_close(_appender_ref), do: :ok

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
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state)

      Application.delete_env(:favn, :connection_mode)
      Application.delete_env(:favn, :query_mode)
      Application.delete_env(:favn, :fetch_mode)
      Application.delete_env(:favn, :rollback_mode)

      if :ets.whereis(@events_table) != :undefined do
        :ets.delete(@events_table)
      end
    end)

    :ets.new(@events_table, [:named_table, :ordered_set, :public])

    Application.put_env(:favn, :connection_mode, :ok)
    Application.put_env(:favn, :query_mode, :ok)
    Application.put_env(:favn, :fetch_mode, :ok)
    Application.put_env(:favn, :rollback_mode, :ok)

    :ok
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

  test "query failure still releases result handle" do
    Application.put_env(:favn, :fetch_mode, :error)

    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert {:error, %Error{type: :execution_error, operation: :query}} =
             DuckDB.query(conn, "SELECT 1", [])

    release_count =
      events()
      |> Enum.count(fn
        {:release, _resource} -> true
        _ -> false
      end)

    assert release_count >= 1
  end

  test "query raise path still releases result handle" do
    Application.put_env(:favn, :fetch_mode, :raise)

    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    assert_raise RuntimeError, "fetch failed", fn ->
      DuckDB.query(conn, "SELECT 1", [])
    end

    assert Enum.any?(events(), fn
             {:release, _resource} -> true
             _ -> false
           end)
  end

  test "transaction rollback failure returns normalized transaction error" do
    Application.put_env(:favn, :query_mode, :error)
    Application.put_env(:favn, :rollback_mode, :error)

    {:ok, conn} = DuckDB.connect(resolved(), duckdb_client: FakeClient)

    write_plan =
      %WritePlan{
        materialization: :incremental,
        strategy: :delete_insert,
        mode: :incremental,
        transactional?: true,
        window: %{start_at: ~U[2025-01-01 00:00:00Z], end_at: ~U[2025-01-02 00:00:00Z]},
        options: %{window_column: "event_at"},
        target: %Relation{schema: "main", name: "orders", type: :table},
        select_sql: "SELECT 1 AS id"
      }

    assert {:error,
            %Error{
              operation: :materialize,
              message: "transaction rollback failed",
              retryable?: false,
              details: %{transaction_stage: :rollback}
            }} = DuckDB.materialize(conn, write_plan, [])
  end

  defp resolved do
    %Resolved{
      name: :duckdb_runtime,
      adapter: DuckDB,
      module: __MODULE__,
      config: %{database: ":memory:"}
    }
  end

  defp events do
    @events_table
    |> :ets.tab2list()
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
