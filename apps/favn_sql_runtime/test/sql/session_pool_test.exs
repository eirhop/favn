defmodule FavnSQLRuntime.SQLSessionPoolTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.Resolved
  alias Favn.SQL.Admission.Limiter
  alias Favn.SQL.{Capabilities, PoolConfig, PoolKey, Session, SessionPool}

  defmodule FakeAdapter do
    def disconnect(%{tracker: tracker}, _opts) do
      Agent.update(tracker, fn state -> %{state | disconnects: state.disconnects + 1} end)
      :ok
    end
  end

  setup do
    {:ok, tracker} = Agent.start_link(fn -> %{disconnects: 0} end)
    pool_name = Module.concat(__MODULE__, "Pool#{System.unique_integer([:positive])}")
    pool_pid = start_supervised!({SessionPool, name: pool_name})

    on_exit(fn -> if Process.alive?(tracker), do: Agent.stop(tracker) end)

    {:ok, tracker: tracker, pool_name: pool_name, pool_pid: pool_pid}
  end

  test "checkin returns an owned session to idle and checkout is exclusive", context do
    key = pool_key(:one)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    session = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(session, name: context.pool_name)
    assert :ok = SessionPool.checkin(session, :ok, name: context.pool_name)

    assert %{active: 0, idle: 1, keys: [%{hash: hash, idle: 1, active: 0}]} =
             SessionPool.diagnostics(name: context.pool_name)

    assert hash == key.hash
    assert {:ok, checked_out} = SessionPool.checkout(key, name: context.pool_name)
    assert %SessionPool.Checkout{key: ^key, owner: owner} = checked_out.pool_checkout
    assert owner == self()
    assert SessionPool.checkout(key, name: context.pool_name) == :miss
  end

  test "max idle cap closes overflow sessions", context do
    key = pool_key(:cap)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}

    first = checked_out_session(context.tracker, key, config)
    second = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(first, name: context.pool_name)
    assert :ok = SessionPool.track_checkout(second, name: context.pool_name)
    assert :ok = SessionPool.checkin(first, :ok, name: context.pool_name)
    assert :ok = SessionPool.checkin(second, :ok, name: context.pool_name)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 1 end)
    assert %{idle: 1} = SessionPool.diagnostics(name: context.pool_name)
  end

  test "discard closes a checked-out session", context do
    key = pool_key(:discard)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    session = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(session, name: context.pool_name)
    assert :ok = SessionPool.discard(session, :failed, name: context.pool_name)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0, keys: []} = SessionPool.diagnostics(name: context.pool_name)
  end

  test "owner death discards checked-out session", context do
    key = pool_key(:owner_down)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    parent = self()

    owner =
      spawn(fn ->
        session = checked_out_session(context.tracker, key, config, self())
        send(parent, {:session, session})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:session, session}
    assert :ok = SessionPool.track_checkout(session, name: context.pool_name)
    send(owner, :stop)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0} = SessionPool.diagnostics(name: context.pool_name)
  end

  test "idle timeout evicts idle sessions", context do
    key = pool_key(:idle)
    config = %PoolConfig{enabled: true, max_idle_per_key: 1, idle_timeout_ms: 10}
    session = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(session, name: context.pool_name)
    assert :ok = SessionPool.checkin(session, :ok, name: context.pool_name)
    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0, keys: []} = SessionPool.diagnostics(name: context.pool_name)
  end

  test "disabled pool closes sessions on checkin", context do
    key = pool_key(:disabled)
    config = %PoolConfig{enabled: false, max_idle_per_key: 1, idle_timeout_ms: 60_000}
    session = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(session, name: context.pool_name)
    assert :ok = SessionPool.checkin(session, :ok, name: context.pool_name)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 1 end)
    assert %{active: 0, idle: 0} = SessionPool.diagnostics(name: context.pool_name)
  end

  test "pool shutdown closes idle and checked-out sessions", context do
    key = pool_key(:shutdown)
    config = %PoolConfig{enabled: true, max_idle_per_key: 2, idle_timeout_ms: 60_000}

    idle = checked_out_session(context.tracker, key, config)
    active = checked_out_session(context.tracker, key, config)

    assert :ok = SessionPool.track_checkout(idle, name: context.pool_name)
    assert :ok = SessionPool.checkin(idle, :ok, name: context.pool_name)
    assert :ok = SessionPool.track_checkout(active, name: context.pool_name)

    GenServer.stop(context.pool_pid, :shutdown)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 2 end)
  end

  test "supervisor shutdown closes sessions and releases admission leases", context do
    pool_name = Module.concat(__MODULE__, "SupervisorPool#{System.unique_integer([:positive])}")
    {:ok, supervisor} = Supervisor.start_link([{SessionPool, name: pool_name}], strategy: :one_for_one)
    key = pool_key(:supervisor_shutdown)
    config = %PoolConfig{enabled: true, max_idle_per_key: 2, idle_timeout_ms: 60_000}
    scope = {:test_scope, make_ref()}

    assert :ok = Limiter.acquire(scope, 1, 50)

    idle = checked_out_session(context.tracker, key, config)
    active = checked_out_session(context.tracker, key, config, self(), {:held, scope, self()})

    assert :ok = SessionPool.track_checkout(idle, name: pool_name)
    assert :ok = SessionPool.checkin(idle, :ok, name: pool_name)
    assert :ok = SessionPool.track_checkout(active, name: pool_name)

    Supervisor.stop(supervisor)

    assert eventually(fn -> Agent.get(context.tracker, & &1.disconnects) == 2 end)
    assert :ok = Limiter.acquire(scope, 1, 50)
    Limiter.release(scope)
  end

  test "creator death clears the single-flight gate and releases a waiter", context do
    key = pool_key(:creator_down)
    parent = self()

    creator =
      spawn(fn ->
        result = SessionPool.checkout_or_create(key, name: context.pool_name)
        send(parent, {:creator_result, result})

        receive do
          :finish -> SessionPool.creation_finished(key, name: context.pool_name)
        end
      end)

    assert_receive {:creator_result, :create}

    waiter =
      spawn(fn ->
        result = SessionPool.checkout_or_create(key, name: context.pool_name)
        send(parent, {:waiter_result, result})

        receive do
          :finish -> SessionPool.creation_finished(key, name: context.pool_name)
        end
      end)

    assert eventually(fn -> SessionPool.diagnostics(name: context.pool_name).waiters == 1 end)
    Process.exit(creator, :kill)

    assert_receive {:waiter_result, :create}, 500
    assert %{creating: 1, waiters: 0} = SessionPool.diagnostics(name: context.pool_name)

    send(waiter, :finish)
    assert eventually(fn -> match?(%{creating: 0, waiters: 0}, SessionPool.diagnostics(name: context.pool_name)) end)
  end

  test "pool keys hash stable inputs and sort required catalogs" do
    resolved = resolved(:stable, %{database: "secret.duckdb"})

    assert PoolKey.build(resolved, [mode: :read], [:mart, "raw"], :v1) ==
             PoolKey.build(resolved, [mode: :read], ["raw", :mart], :v1)

    refute PoolKey.build(resolved, [mode: :write], [:mart, "raw"], :v1) ==
             PoolKey.build(resolved, [mode: :read], ["raw", :mart], :v1)

    assert %PoolKey{hash: hash} = PoolKey.build(resolved, [], [], nil)
    assert is_binary(hash)
    refute String.contains?(hash, "secret")
  end

  defp checked_out_session(tracker, key, config, owner \\ self(), lease \\ nil) do
    tracker
    |> session(lease)
    |> SessionPool.attach_checkout(key, config, owner)
  end

  defp session(tracker, lease) do
    %Session{
      adapter: FakeAdapter,
      resolved: resolved(:warehouse, %{tracker: tracker}),
      conn: %{tracker: tracker},
      capabilities: %Capabilities{},
      admission_lease: lease
    }
  end

  defp pool_key(name), do: PoolKey.build(resolved(name, %{}), [], [], nil)

  defp resolved(name, config) do
    %Resolved{name: name, adapter: FakeAdapter, module: __MODULE__, config: config}
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
