defmodule FavnRunner.ControlPlaneConnectionTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FavnRunner.ControlPlaneConnection

  @target :"control@control.internal"

  test "connects only to the explicit configured control-plane node" do
    owner = self()

    connection =
      start_connection(
        connect_fun: fn node_name ->
          send(owner, {:connection_attempt, node_name})
          node_name == @target
        end
      )

    assert_receive {:connection_attempt, @target}

    assert_eventually(fn ->
      ControlPlaneConnection.gateway(connection) ==
        {:ok, {:"Elixir.FavnOrchestrator.RunnerGateway", @target}}
    end)

    assert %{
             status: :connected,
             connected?: true,
             target_node: "control@control.internal",
             retry_count: 0
           } = ControlPlaneConnection.diagnostics(connection)
  end

  test "rejects malformed and dotless node names before attempting distribution" do
    Process.flag(:trap_exit, true)

    for invalid <- [
          "not a node",
          "control@short-host",
          "control@bad_host.internal",
          "control@127.0.0.2"
        ] do
      assert {:error, :invalid_control_plane_node} =
               ControlPlaneConnection.start_link(name: nil, node: invalid)
    end
  end

  test "coalesces reconnect requests and exposes a safe bounded failure" do
    owner = self()

    connection =
      start_connection(
        connect_fun: fn node_name ->
          send(owner, {:failed_attempt, node_name})
          false
        end,
        retry_delay_fun: fn _attempt -> 1_000 end
      )

    assert_receive {:failed_attempt, @target}

    assert_eventually(fn ->
      match?(
        %{
          status: :connecting,
          connected?: false,
          retry_count: 1,
          last_failure_class: :distribution_handshake_failed,
          next_retry_ms: 1_000
        },
        ControlPlaneConnection.diagnostics(connection)
      )
    end)

    Enum.each(1..20, fn _attempt -> ControlPlaneConnection.reconnect(connection) end)
    refute_receive {:failed_attempt, @target}, 100
  end

  test "caps retry delay and reconnects successfully after a transient failure" do
    attempts = :atomics.new(1, [])
    monitor_calls = :atomics.new(1, [])

    connection =
      start_connection(
        connect_fun: fn _node -> :atomics.add_get(attempts, 1, 1) > 1 end,
        monitor_fun: fn ->
          :atomics.add_get(monitor_calls, 1, 1)
          :ok
        end,
        retry_delay_fun: fn _attempt -> 999_999 end
      )

    assert_eventually(fn ->
      match?(
        %{status: :connecting, retry_count: 1, next_retry_ms: 30_000},
        ControlPlaneConnection.diagnostics(connection)
      )
    end)

    send(connection, {:connect, :stale})
    ControlPlaneConnection.reconnect(connection)

    # The scheduled timer remains authoritative; reconnect requests do not
    # bypass it. Model its eventual delivery without waiting 30 seconds.
    state = :sys.get_state(connection)
    send(connection, {:connect, state.retry_token})

    assert_eventually(fn ->
      match?(
        %{
          status: :connected,
          connected?: true,
          retry_count: 0,
          last_failure_class: :distribution_handshake_failed
        },
        ControlPlaneConnection.diagnostics(connection)
      )
    end)

    assert :atomics.get(monitor_calls, 1) == 1
  end

  test "rate-limits warnings while telemetry records every failed attempt" do
    handler = "runner-control-plane-#{System.unique_integer([:positive])}"
    owner = self()
    previous_log_level = Logger.level()
    Logger.configure(level: :warning)

    :ok =
      :telemetry.attach(
        handler,
        [:favn, :runner, :control_plane_connection_failed],
        fn event, measurements, metadata, _config ->
          send(owner, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler)
      Logger.configure(level: previous_log_level)
    end)

    log =
      capture_log(fn ->
        connection =
          start_connection(connect_fun: fn _node -> false end, retry_delay_fun: fn _ -> 2 end)

        assert_eventually(fn ->
          ControlPlaneConnection.diagnostics(connection).retry_count >= 20
        end)

        :ok = stop_supervised(ControlPlaneConnection)
        Logger.flush()
      end)

    assert log =~ "favn.runner.control_plane_connection_failed"
    assert length(Regex.scan(~r/favn\.runner\.control_plane_connection_failed/, log)) == 5
    assert log =~ "retry_count: 20"

    for retry_count <- 1..20 do
      assert_receive {[:favn, :runner, :control_plane_connection_failed],
                      %{retry_count: ^retry_count},
                      %{
                        failure_class: :distribution_handshake_failed,
                        target_node: "control@control.internal"
                      }}
    end

    refute log =~ "cookie"
    refute log =~ "certificate"
    refute log =~ "/etc/"
  end

  test "publishes connection loss and the bounded OTP failure class" do
    connection = start_connection()
    assert :ok = ControlPlaneConnection.subscribe(connection, self())

    assert_receive {:favn_control_plane_connection, %{status: status}}

    if status == :connecting,
      do: assert_receive({:favn_control_plane_connection, %{status: :connected}})

    send(
      connection,
      {:nodedown, @target, %{nodedown_reason: :net_tick_timeout, secret: "not-forwarded"}}
    )

    assert_receive {:favn_control_plane_connection,
                    %{
                      status: :connecting,
                      connected?: false,
                      last_failure_class: :node_tick_timeout
                    }}

    diagnostics = ControlPlaneConnection.diagnostics(connection)
    refute inspect(diagnostics) =~ "not-forwarded"

    send(connection, {:nodedown, @target, %{nodedown_reason: :connection_closed}})
    assert ControlPlaneConnection.diagnostics(connection).retry_count == 1
  end

  test "duplicate node-up notifications do not duplicate the connected event" do
    handler = "runner-connected-#{System.unique_integer([:positive])}"
    owner = self()

    :ok =
      :telemetry.attach(
        handler,
        [:favn, :runner, :control_plane_connected],
        fn event, measurements, metadata, _config ->
          send(owner, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    connection = start_connection()

    assert_receive {[:favn, :runner, :control_plane_connected], _measurements,
                    %{target_node: "control@control.internal"}}

    send(connection, {:nodeup, @target, %{}})
    refute_receive {[:favn, :runner, :control_plane_connected], _, _}, 100
  end

  test "probe exceptions are reduced to a safe class without leaking canaries" do
    canary = "cookie-token /private/tls/client.key certificate-body"

    log =
      capture_log(fn ->
        connection =
          start_connection(
            probe_fun: fn _node -> raise canary end,
            retry_delay_fun: fn _attempt -> 1_000 end
          )

        assert_eventually(fn ->
          ControlPlaneConnection.diagnostics(connection).last_failure_class ==
            :connection_probe_failed
        end)

        refute inspect(ControlPlaneConnection.diagnostics(connection)) =~ canary
        :ok = stop_supervised(ControlPlaneConnection)
        Logger.flush()
      end)

    refute log =~ canary
  end

  defp start_connection(opts \\ []) do
    defaults = [
      name: nil,
      node: @target,
      probe_fun: fn _node -> :ok end,
      connect_fun: fn _node -> true end,
      monitor_fun: fn -> :ok end,
      retry_delay_fun: fn _attempt -> 10 end
    ]

    start_supervised!({ControlPlaneConnection, Keyword.merge(defaults, opts)})
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
