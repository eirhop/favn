defmodule FavnDuckdbTest do
  use ExUnit.Case, async: false

  alias FavnDuckdb.Runtime
  alias FavnDuckdb.Runtime.SeparateProcess
  alias FavnDuckdb.Worker

  defmodule FakeClient do
    use FavnDuckdb.TestSupport.FakeClient

    @impl true
    def fetch_all(_result_ref), do: [[1]]

    @impl true
    def columns(_result_ref), do: ["id"]
  end

  defmodule SlowClient do
    use FavnDuckdb.TestSupport.FakeClient

    @impl true
    def query(_conn_ref, _sql, _params) do
      Process.sleep(100)
      {:ok, make_ref()}
    end
  end

  defmodule CloseFailsOnceClient do
    use FavnDuckdb.TestSupport.FakeClient

    @impl true
    def appender_close(appender_ref) do
      key = {__MODULE__, appender_ref}
      close_count = Process.get(key, 0)
      Process.put(key, close_count + 1)

      case close_count do
        0 -> {:error, :temporary_appender_close_failed}
        _ -> :ok
      end
    end
  end

  defmodule CloseAlwaysFailsClient do
    use FavnDuckdb.TestSupport.FakeClient

    @impl true
    def appender_close(_appender_ref), do: {:error, :appender_close_failed}

    @impl true
    def release(resource) do
      send(Application.fetch_env!(:favn, :duckdb_test_owner), {:released, resource})
      :ok
    end
  end

  setup do
    previous_plugins = Application.get_env(:favn, :runner_plugins)
    previous_in_process_client = Application.get_env(:favn, :duckdb_in_process_client)
    previous_test_owner = Application.get_env(:favn, :duckdb_test_owner)

    on_exit(fn ->
      if is_nil(previous_plugins) do
        Application.delete_env(:favn, :runner_plugins)
      else
        Application.put_env(:favn, :runner_plugins, previous_plugins)
      end

      case Process.whereis(Worker) do
        nil ->
          :ok

        _pid ->
          try do
            GenServer.stop(Worker, :normal, 1_000)
          catch
            :exit, {:noproc, _} -> :ok
          end
      end

      if is_nil(previous_in_process_client) do
        Application.delete_env(:favn, :duckdb_in_process_client)
      else
        Application.put_env(:favn, :duckdb_in_process_client, previous_in_process_client)
      end

      if is_nil(previous_test_owner) do
        Application.delete_env(:favn, :duckdb_test_owner)
      else
        Application.put_env(:favn, :duckdb_test_owner, previous_test_owner)
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

  test "runtime config falls back to safe defaults for invalid plugin options" do
    Application.put_env(:favn, :runner_plugins, [
      {FavnDuckdb,
       execution_mode: :unknown_mode, worker_name: "invalid", worker_call_timeout: :invalid}
    ])

    assert Runtime.execution_mode() == :in_process
    assert Runtime.worker_name() == FavnDuckdb.Worker
    assert Runtime.worker_call_timeout() == :infinity
    assert Runtime.client_module() == FavnDuckdb.Runtime.InProcess
  end

  test "worker returns invalid_handle for unknown references in separate_process mode" do
    Application.put_env(:favn, :runner_plugins, [{FavnDuckdb, execution_mode: :separate_process}])
    {:ok, _pid} = Worker.start_link(name: Worker, client: FakeClient)

    assert {:error, :invalid_handle} = SeparateProcess.query(make_ref(), "SELECT 1", [])
    assert {:error, :invalid_handle} = SeparateProcess.fetch_all(make_ref())
    assert {:error, :invalid_handle} = SeparateProcess.columns(make_ref())
    assert :ok = SeparateProcess.release(make_ref())
  end

  test "successful appender close consumes separate-process worker handle" do
    {:ok, _pid} = start_separate_process_worker(FakeClient)
    {:ok, appender_ref} = open_appender()

    assert :ok = SeparateProcess.appender_close(appender_ref)
    assert {:error, :invalid_handle} = SeparateProcess.appender_flush(appender_ref)
  end

  test "failed appender close keeps separate-process worker handle retryable" do
    {:ok, _pid} = start_separate_process_worker(CloseFailsOnceClient)
    {:ok, appender_ref} = open_appender()

    assert {:error, :temporary_appender_close_failed} =
             SeparateProcess.appender_close(appender_ref)

    assert :ok = SeparateProcess.appender_close(appender_ref)
    assert {:error, :invalid_handle} = SeparateProcess.appender_flush(appender_ref)
  end

  test "failed appender close keeps separate-process worker handle releasable" do
    Application.put_env(:favn, :duckdb_test_owner, self())
    {:ok, _pid} = start_separate_process_worker(CloseAlwaysFailsClient)
    {:ok, appender_ref} = open_appender()

    assert {:error, :appender_close_failed} = SeparateProcess.appender_close(appender_ref)
    assert :ok = SeparateProcess.release(appender_ref)
    assert_receive {:released, _resource}
    assert {:error, :invalid_handle} = SeparateProcess.appender_flush(appender_ref)
  end

  defp start_separate_process_worker(client) do
    Application.put_env(:favn, :runner_plugins, [{FavnDuckdb, execution_mode: :separate_process}])
    Worker.start_link(name: Worker, client: client)
  end

  defp open_appender do
    {:ok, db_ref} = SeparateProcess.open(":memory:")
    {:ok, conn_ref} = SeparateProcess.connection(db_ref)
    SeparateProcess.appender(conn_ref, "orders", nil)
  end
end
