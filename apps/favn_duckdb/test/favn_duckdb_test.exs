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

  setup do
    previous_plugins = Application.get_env(:favn, :runner_plugins)

    on_exit(fn ->
      if is_nil(previous_plugins) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous_plugins)
      end

      if Process.whereis(Worker) do
        GenServer.stop(Worker, :normal, 1_000)
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
end
