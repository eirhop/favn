defmodule FavnSQLRuntime.SQLClientTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, Client, PoolConfig, PoolKey, Session, SessionPool}

  defmodule BlockingAdapter do
    def query(%{tracker: tracker}, _statement, _opts) do
      Agent.update(tracker, &Map.put(&1, :query_started?, true))
      Process.sleep(:infinity)
    end

    def disconnect(%{tracker: tracker}, _opts) do
      Agent.update(tracker, &Map.update!(&1, :disconnects, fn count -> count + 1 end))
      :ok
    end
  end

  defmodule ErrorReturningAdapter do
    def query(_conn, _statement, _opts), do: {:error, :adapter_down}
  end

  defmodule TransactionBlockingAdapter do
    def transaction(%{tracker: tracker} = conn, fun, _opts) do
      caller = self()
      Agent.update(tracker, &Map.put(&1, :transaction_pid, caller))
      fun.(conn)
    end

    def query(%{tracker: tracker}, _statement, _opts) do
      caller = self()
      Agent.update(tracker, &Map.put(&1, :nested_query_pid, caller))
      Process.sleep(:infinity)
    end

    def disconnect(%{tracker: tracker}, _opts) do
      Agent.update(tracker, &Map.update!(&1, :disconnects, fn count -> count + 1 end))
      :ok
    end
  end

  setup do
    :ok = SessionPool.reset()
    {:ok, tracker} = Agent.start_link(fn -> %{query_started?: false, disconnects: 0} end)

    on_exit(fn ->
      SessionPool.reset()
      if Process.alive?(tracker), do: Agent.stop(tracker)
    end)

    {:ok, tracker: tracker}
  end

  test "query returns operation timeout when adapter blocks", %{tracker: tracker} do
    session = session(tracker)

    assert {:error,
            %Favn.SQL.Error{
              type: :operation_timeout,
              operation: :query,
              adapter: BlockingAdapter,
              retryable?: nil,
              details: %{timeout_ms: 10, unknown_outcome?: true}
            }} = Client.query(session, "select 1", timeout_ms: 10, read_only?: true)

    assert Agent.get(tracker, & &1.query_started?)
  end

  test "pooled session is discarded after operation timeout", %{tracker: tracker} do
    key = pool_key(:timeout_discard)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    session = SessionPool.attach_checkout(session(tracker), key, config)

    assert :ok = SessionPool.track_checkout(session)

    assert {:error, %Favn.SQL.Error{type: :operation_timeout}} =
             Client.query(session, "select 1", timeout_ms: 10, read_only?: true)

    assert :ok = Client.disconnect(session)
    assert eventually(fn -> Agent.get(tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0} = SessionPool.diagnostics()
  end

  test "adapter error returns are normalized under deadline wrapper", %{tracker: tracker} do
    session = %Session{session(tracker) | adapter: ErrorReturningAdapter}

    assert {:error,
            %Favn.SQL.Error{
              type: :execution_error,
              operation: :query,
              cause: :adapter_down
            }} = Client.query(session, "select 1", timeout_ms: 1_000, read_only?: true)
  end

  test "transaction timeout kills the process running a nested operation and discards pooling", %{
    tracker: tracker
  } do
    session = transaction_session(tracker)
    key = pool_key(:nested_timeout_discard)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    session = SessionPool.attach_checkout(session, key, config)

    assert :ok = SessionPool.track_checkout(session)

    assert {:error, %Favn.SQL.Error{type: :operation_timeout, operation: :transaction}} =
             Client.transaction(
               session,
               fn tx_session ->
                 Client.query(tx_session, "select blocked", timeout_ms: 5_000, read_only?: true)
               end,
               timeout_ms: 1_000
             )

    nested_query_pid = Agent.get(tracker, &Map.fetch!(&1, :nested_query_pid))
    transaction_pid = Agent.get(tracker, &Map.fetch!(&1, :transaction_pid))
    assert nested_query_pid == transaction_pid
    refute Process.alive?(nested_query_pid)

    assert :ok = Client.disconnect(session)
    assert eventually(fn -> Agent.get(tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0} = SessionPool.diagnostics()
  end

  test "a shorter nested timeout cancels the transaction worker", %{tracker: tracker} do
    session = transaction_session(tracker)

    assert {:error,
            %Favn.SQL.Error{
              type: :operation_timeout,
              operation: :query,
              details: %{timeout_ms: 100, unknown_outcome?: true}
            }} =
             Client.transaction(
               session,
               fn tx_session ->
                 Client.query(tx_session, "select blocked", timeout_ms: 100, read_only?: true)
               end,
               timeout_ms: 1_000
             )

    nested_query_pid = Agent.get(tracker, &Map.fetch!(&1, :nested_query_pid))
    transaction_pid = Agent.get(tracker, &Map.fetch!(&1, :transaction_pid))
    assert nested_query_pid == transaction_pid
    refute Process.alive?(nested_query_pid)
  end

  test "caller cancellation kills the process running a nested transaction operation", %{
    tracker: tracker
  } do
    session = transaction_session(tracker)

    task =
      Task.async(fn ->
        Client.transaction(
          session,
          fn tx_session ->
            Client.query(tx_session, "select blocked", timeout_ms: 30_000, read_only?: true)
          end,
          timeout_ms: 30_000
        )
      end)

    assert eventually(fn -> Agent.get(tracker, &Map.has_key?(&1, :nested_query_pid)) end)
    nested_query_pid = Agent.get(tracker, &Map.fetch!(&1, :nested_query_pid))
    transaction_pid = Agent.get(tracker, &Map.fetch!(&1, :transaction_pid))
    assert nested_query_pid == transaction_pid

    assert nil == Task.shutdown(task, :brutal_kill)
    assert eventually(fn -> not Process.alive?(nested_query_pid) end)
  end

  defp session(tracker) do
    %Session{
      adapter: BlockingAdapter,
      resolved: resolved(:warehouse, %{tracker: tracker}),
      conn: %{tracker: tracker},
      capabilities: %Capabilities{}
    }
  end

  defp transaction_session(tracker) do
    %Session{
      session(tracker)
      | adapter: TransactionBlockingAdapter,
        resolved: %Resolved{
          name: :warehouse,
          adapter: TransactionBlockingAdapter,
          module: __MODULE__,
          config: %{tracker: tracker}
        },
        capabilities: %Capabilities{transactions: :supported}
    }
  end

  defp pool_key(name), do: PoolKey.build(resolved(name, %{}), [], [], nil)

  defp resolved(name, config) do
    %Resolved{name: name, adapter: BlockingAdapter, module: __MODULE__, config: config}
  end

  defp eventually(fun, attempts \\ 20)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
