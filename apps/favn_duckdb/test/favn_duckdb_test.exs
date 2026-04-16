defmodule FavnDuckdbTest do
  use ExUnit.Case, async: false

  alias FavnDuckdb.Runtime.SeparateProcess
  alias FavnDuckdb.Worker

  defmodule FakeClient do
    @behaviour Favn.SQL.Adapter.DuckDB.Client

    @impl true
    def open(_database), do: {:ok, make_ref()}

    @impl true
    def connection(_db_ref), do: {:ok, make_ref()}

    @impl true
    def query(_conn_ref, _sql, _params), do: {:ok, make_ref()}

    @impl true
    def fetch_all(_result_ref), do: [[1]]

    @impl true
    def columns(_result_ref), do: ["id"]

    @impl true
    def begin_transaction(_conn_ref), do: :ok

    @impl true
    def commit(_conn_ref), do: :ok

    @impl true
    def rollback(_conn_ref), do: :ok

    @impl true
    def appender(_conn_ref, _table_name, _schema), do: {:ok, make_ref()}

    @impl true
    def appender_add_rows(_appender_ref, _rows), do: :ok

    @impl true
    def appender_flush(_appender_ref), do: :ok

    @impl true
    def appender_close(_appender_ref), do: :ok

    @impl true
    def release(_resource), do: :ok
  end

  defmodule SlowClient do
    @behaviour Favn.SQL.Adapter.DuckDB.Client

    @impl true
    def open(_database), do: {:ok, make_ref()}

    @impl true
    def connection(_db_ref), do: {:ok, make_ref()}

    @impl true
    def query(_conn_ref, _sql, _params) do
      Process.sleep(100)
      {:ok, make_ref()}
    end

    @impl true
    def fetch_all(_result_ref), do: []

    @impl true
    def columns(_result_ref), do: []

    @impl true
    def begin_transaction(_conn_ref), do: :ok

    @impl true
    def commit(_conn_ref), do: :ok

    @impl true
    def rollback(_conn_ref), do: :ok

    @impl true
    def appender(_conn_ref, _table_name, _schema), do: {:ok, make_ref()}

    @impl true
    def appender_add_rows(_appender_ref, _rows), do: :ok

    @impl true
    def appender_flush(_appender_ref), do: :ok

    @impl true
    def appender_close(_appender_ref), do: :ok

    @impl true
    def release(_resource), do: :ok
  end

  setup do
    previous_plugins = Application.get_env(:favn, :runner_plugins)
    previous_in_process_client = Application.get_env(:favn, :duckdb_in_process_client)

    on_exit(fn ->
      if is_nil(previous_plugins) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous_plugins)
      end

      if Process.whereis(Worker) do
        GenServer.stop(Worker, :normal, 1_000)
      end

      if is_nil(previous_in_process_client) do
        Application.delete_env(:favn, :duckdb_in_process_client)
      else
        Application.put_env(:favn, :duckdb_in_process_client, previous_in_process_client)
      end
    end)

    :ok
  end

  test "plugin has no child specs in in_process mode" do
    assert [] == FavnDuckdb.child_specs(execution_mode: :in_process)
  end

  test "plugin starts a long-lived worker in separate_process mode" do
    [worker_spec] = FavnDuckdb.child_specs(execution_mode: :separate_process)

    assert %{start: {Worker, :start_link, [_opts]}} =
             Supervisor.child_spec(worker_spec, [])

    {:ok, _pid} = Worker.start_link(name: Worker, client: FakeClient)
    assert Process.alive?(Process.whereis(Worker))
  end

  test "separate_process runtime returns worker_not_available when worker is missing" do
    Application.put_env(:favn, :runner_plugins, [{FavnDuckdb, execution_mode: :separate_process}])

    assert {:error, :worker_not_available} = SeparateProcess.open(":memory:")
  end

  test "separate_process runtime reuses worker for coarse execution calls" do
    Application.put_env(:favn, :runner_plugins, [{FavnDuckdb, execution_mode: :separate_process}])
    {:ok, _pid} = Worker.start_link(name: Worker, client: FakeClient)

    assert {:ok, db_ref} = SeparateProcess.open(":memory:")
    assert {:ok, conn_ref} = SeparateProcess.connection(db_ref)
    assert {:ok, result_ref} = SeparateProcess.query(conn_ref, "SELECT 1", [])
    assert [[1]] == SeparateProcess.fetch_all(result_ref)
    assert ["id"] == SeparateProcess.columns(result_ref)
    assert :ok == SeparateProcess.release(result_ref)
    assert :ok == SeparateProcess.release(conn_ref)
    assert :ok == SeparateProcess.release(db_ref)
  end

  test "in_process appender supports schema-qualified tables" do
    alias FavnDuckdb.Runtime.InProcess

    assert {:ok, db_ref} = InProcess.open(":memory:")
    assert {:ok, conn_ref} = InProcess.connection(db_ref)
    assert {:ok, create_schema_ref} = InProcess.query(conn_ref, "CREATE SCHEMA analytics", [])
    assert :ok = InProcess.release(create_schema_ref)

    assert {:ok, create_table_ref} =
             InProcess.query(conn_ref, "CREATE TABLE analytics.orders (id BIGINT)", [])

    assert :ok = InProcess.release(create_table_ref)
    assert {:ok, appender_ref} = InProcess.appender(conn_ref, "orders", "analytics")
    assert :ok = InProcess.appender_add_rows(appender_ref, [[1], [2]])
    assert :ok = InProcess.appender_flush(appender_ref)
    assert :ok = InProcess.appender_close(appender_ref)

    assert {:ok, count_ref} =
             InProcess.query(conn_ref, "SELECT COUNT(*) AS c FROM analytics.orders", [])

    [count_row] = InProcess.fetch_all(count_ref)

    count =
      case count_row do
        %{} = row -> row |> Map.values() |> List.first()
        [value] -> value
      end

    assert count == 2
    assert :ok = InProcess.release(count_ref)
    assert :ok = InProcess.release(conn_ref)
    assert :ok = InProcess.release(db_ref)
  end

  test "separate_process runtime uses configured worker call timeout" do
    Application.put_env(:favn, :runner_plugins, [
      {FavnDuckdb, execution_mode: :separate_process, worker_call_timeout: 10}
    ])

    {:ok, _pid} = Worker.start_link(name: Worker, client: SlowClient)
    assert {:ok, db_ref} = SeparateProcess.open(":memory:")
    assert {:ok, conn_ref} = SeparateProcess.connection(db_ref)

    assert {:error, :worker_call_timeout} = SeparateProcess.query(conn_ref, "SELECT 1", [])
  end
end
