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

  defp session(tracker) do
    %Session{
      adapter: BlockingAdapter,
      resolved: resolved(:warehouse, %{tracker: tracker}),
      conn: %{tracker: tracker},
      capabilities: %Capabilities{}
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
