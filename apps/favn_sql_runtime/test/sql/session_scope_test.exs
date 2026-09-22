defmodule FavnSQLRuntime.SessionScopeTest do
  use ExUnit.Case, async: false
  alias Favn.Connection.{Registry, Resolved}
  alias Favn.SQL.{Capabilities, Client, Deadline, Error, PoolConfig, SessionPool}

  defmodule TrappingNative do
    use GenServer

    def init(observer) do
      Process.flag(:trap_exit, true)
      {:ok, observer}
    end

    def handle_call(:block, _from, observer) do
      send(observer, :native_blocked)

      receive do
        :release -> send(observer, :native_continued)
      end

      {:reply, :ok, observer}
    end
  end

  defmodule Adapter do
    def connect(%{config: config}, _) do
      {:ok, native} =
        if config[:trapping_native],
          do: GenServer.start_link(TrappingNative, config.observer),
          else: Agent.start_link(fn -> :native end)

      send(config.observer, {:allocated, self(), native})
      if config[:connect_block], do: Process.sleep(:infinity)
      {:ok, Map.put(config, :native, native)}
    end

    def bootstrap(config, _, _) do
      if config[:bootstrap_block], do: Process.sleep(:infinity)
      :ok
    end

    def disconnect(config, _) do
      send(config.observer, {:disconnect, self()})

      if config[:disconnect_block] == true and Process.alive?(config.native),
        do: Process.sleep(:infinity)

      if Process.alive?(config.native), do: Agent.stop(config.native)
      :ok
    end

    def concurrency_policies(resolved),
      do: {:ok, [Favn.SQL.ConcurrencyPolicy.catalog(resolved, "mart", 1)]}

    def query(%{trapping_native: true, native: native}, _, _),
      do: GenServer.call(native, :block, :infinity)

    def query(_, _, _), do: Process.sleep(:infinity)
    def capabilities(_, _), do: {:ok, %Capabilities{}}
    def poolable?(_, _), do: true
    def pool_fingerprint(_, _), do: :scope
    def reset_session(_, _, _), do: :ok
  end

  setup do
    SessionPool.reset()
    on_exit(fn -> SessionPool.reset() end)
    :ok
  end

  defp registry(config) do
    resolved = %Resolved{
      name: :scope,
      module: __MODULE__,
      adapter: Adapter,
      config: Map.merge(%{observer: self(), pool: %PoolConfig{enabled: true}}, config),
      secret_fields: []
    }

    start_supervised!({Registry, name: __MODULE__.Registry, connections: %{scope: resolved}})
    [registry_name: __MODULE__.Registry]
  end

  test "acquisition deadline reaps partial native creation and never admits callback" do
    for config <- [%{connect_block: true}, %{bootstrap_block: true}] do
      opts = registry(config)
      parent = self()

      assert {:error, %Error{details: %{session_phase: :acquiring, unknown_outcome?: false}}} =
               Client.with_session(:scope, opts ++ [timeout_ms: 100], fn _ ->
                 send(parent, :callback)
               end)

      assert_receive {:allocated, worker, native}
      refute Process.alive?(worker)
      native_monitor = Process.monitor(native)
      assert_receive {:DOWN, ^native_monitor, :process, ^native, _}
      refute_receive :callback
      assert %{active: 0, idle: 0} = SessionPool.diagnostics()
      stop_supervised!(Registry)
    end
  end

  test "qualification expires before acknowledged admission" do
    opts = registry(%{})
    parent = self()

    prepare = fn _ ->
      send(parent, :qualifying)
      Process.sleep(:infinity)
    end

    assert {:error, %Error{details: %{session_phase: :acquiring, unknown_outcome?: false}}} =
             Client.with_session(:scope, opts ++ [timeout_ms: 100, prepare: prepare], fn _ ->
               send(parent, :callback)
             end)

    assert_receive :qualifying
    refute_receive :callback
  end

  test "timeout after admission is unknown and kills owner" do
    opts = registry(%{})
    parent = self()

    assert {:error, %Error{details: %{session_phase: :admitted, unknown_outcome?: true}}} =
             Client.with_session(:scope, opts ++ [timeout_ms: 100], fn _ ->
               send(parent, {:callback, self()})
               Process.sleep(:infinity)
             end)

    assert_receive {:callback, worker}
    refute Process.alive?(worker)
  end

  test "completed commit and rejection survive stalled disconnect" do
    for result <- [
          {:ok, :receipt},
          {:error, %Error{type: :transaction_conflict, message: "rejected"}}
        ] do
      opts = registry(%{disconnect_block: true})

      expected =
        case result do
          {:error, error} -> {:error, %{error | details: %{session_phase: :admitted}}}
          result -> result
        end

      assert ^expected =
               Client.with_session(:scope, opts ++ [timeout_ms: 100], fn _ -> result end)

      assert_receive {:allocated, worker, _}
      refute Process.alive?(worker)
      stop_supervised!(Registry)
    end
  end

  test "caller death terminates admitted work and native child" do
    opts = registry(%{})
    parent = self()

    {caller, monitor} =
      spawn_monitor(fn ->
        Client.with_session(:scope, opts ++ [timeout_ms: 5000], fn _ ->
          send(parent, :callback)
          Process.sleep(:infinity)
        end)
      end)

    assert_receive {:allocated, worker, native}
    assert_receive :callback
    worker_monitor = Process.monitor(worker)
    native_monitor = Process.monitor(native)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^caller, :killed}
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _}
    assert_receive {:DOWN, ^native_monitor, :process, ^native, _}
  end

  test "pool capacity and admission capacity expire without late callbacks or reservations" do
    for pool <- [true, false] do
      opts = registry(%{}) ++ [required_catalogs: ["mart"], pool: pool]
      assert {:ok, held} = Client.connect(:scope, opts)
      assert_receive {:allocated, _, _}
      parent = self()

      assert {:error, _} =
               Client.with_session(:scope, opts ++ [timeout_ms: 100], fn _ ->
                 send(parent, :callback)
               end)

      refute_receive :callback
      Client.disconnect(held)
      SessionPool.reset()

      assert {:ok, :after_release} =
               Client.with_session(:scope, opts ++ [timeout_ms: 1000], fn _ ->
                 {:ok, :after_release}
               end)

      refute_receive :callback
      assert %{active: 0, idle: 0} = SessionPool.diagnostics()
      stop_supervised!(Registry)
    end
  end

  test "scope cannot borrow an idle native handle from a previous owner" do
    opts = registry(%{}) ++ [required_catalogs: ["mart"]]
    assert {:ok, legacy} = Client.connect(:scope, opts)
    assert_receive {:allocated, _, old_native}
    Client.disconnect(legacy)

    assert {:ok, new_native} =
             Client.with_session(:scope, opts, fn session ->
               {:ok, session.conn.native}
             end)

    refute old_native == new_native
    refute Process.alive?(new_native)
  end

  test "shorter operation deadline remains effective inside a scope" do
    opts = registry(%{})

    assert {:error, %Error{type: :operation_timeout, details: %{unknown_outcome?: true}}} =
             Client.with_session(:scope, opts ++ [timeout_ms: 5000], fn session ->
               Client.query(session, "SELECT 1", timeout_ms: 20)
             end)
  end

  test "inner timeout reaps a blocked native child that traps owner exits before returning" do
    opts = registry(%{trapping_native: true})

    assert {:error, %Error{type: :operation_timeout, details: %{unknown_outcome?: true}}} =
             Client.with_session(:scope, opts ++ [timeout_ms: 5000, pool: false], fn session ->
               Client.query(session, "SELECT 1", timeout_ms: 100)
             end)

    assert_receive :native_blocked
    assert_receive {:allocated, worker, native}
    refute Process.alive?(worker)
    refute Process.alive?(native)
    send(native, :release)
    refute_receive :native_continued
  end

  test "already expired scope cannot invoke callback" do
    opts = registry(%{})
    deadline = %{Deadline.new(100) | deadline_at: System.monotonic_time(:millisecond) - 1}
    parent = self()

    assert {:error, _} =
             Client.with_session(:scope, opts ++ [deadline: deadline], fn _ ->
               send(parent, :callback)
             end)

    refute_receive :callback
  end
end
