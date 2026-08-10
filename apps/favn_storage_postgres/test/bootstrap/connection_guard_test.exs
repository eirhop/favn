defmodule FavnStoragePostgres.Bootstrap.ConnectionGuardTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.Bootstrap.ConnectionGuard

  test "connection loss is classified when its DOWN arrives before the worker DOWN" do
    assert_order_independent_connection_loss(:connection_first)
  end

  test "connection loss is classified when the worker DOWN arrives first" do
    assert_order_independent_connection_loss(:worker_first)
  end

  test "authentication lifecycle loss is classified without exiting the worker owner" do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        result =
          ConnectionGuard.run(
            connection,
            authentication,
            fn resource, reason ->
              send(test_process, {:classified, resource, reason})
              {:error, :authentication_unavailable}
            end,
            fn ->
              send(test_process, {:guard_worker, self()})
              receive do: (:continue -> :ok)
            end
          )

        send(test_process, {:guard_result, result})
      end)

    assert_receive {:guard_worker, worker}
    Process.exit(authentication, :shutdown)

    assert_receive {:classified, :authentication, :shutdown}
    assert_receive {:guard_result, {:error, :authentication_unavailable}}
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}
    refute Process.alive?(worker)

    Process.exit(connection, :kill)
  end

  test "an independent worker crash remains an unexpected worker exit" do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        ConnectionGuard.run(
          connection,
          authentication,
          fn resource, reason ->
            send(test_process, {:classified, resource, reason})
            {:error, resource}
          end,
          fn ->
            send(test_process, {:guard_worker, self()})
            receive do: (:continue -> :ok)
          end
        )
      end)

    assert_receive {:guard_worker, worker}
    Process.exit(worker, :kill)

    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}
    refute_receive {:classified, _resource, _reason}

    Process.exit(connection, :kill)
    Process.exit(authentication, :kill)
  end

  test "unknown-outcome authentication loss terminates the surviving database resource" do
    assert_unknown_outcome_cleanup(:authentication)
  end

  test "unknown-outcome database loss terminates the surviving authentication resource" do
    assert_unknown_outcome_cleanup(:connection)
  end

  test "unknown-outcome setup cleans every survivor when a resource is already down" do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()
    authentication_monitor = Process.monitor(authentication)
    Process.exit(authentication, :shutdown)
    assert_receive {:DOWN, ^authentication_monitor, :process, ^authentication, :shutdown}

    connection_monitor = Process.monitor(connection)

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        ConnectionGuard.run_unknown_outcome([connection, authentication], fn ->
          send(test_process, :unexpected_guard_callback)
        end)
      end)

    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}
    assert_receive {:DOWN, ^connection_monitor, :process, ^connection, :killed}
    refute_receive :unexpected_guard_callback
  end

  test "normal unknown-outcome guard teardown leaves cleanup to its owner" do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        result =
          ConnectionGuard.run_unknown_outcome([connection, authentication], fn ->
            send(test_process, :unknown_outcome_guard_ready)
            :ok
          end)

        send(test_process, {:unknown_outcome_guard_result, result})
      end)

    assert_receive :unknown_outcome_guard_ready, 1_000
    assert_receive {:unknown_outcome_guard_result, :ok}
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}
    assert Process.alive?(connection)
    assert Process.alive?(authentication)

    Process.exit(connection, :kill)
    Process.exit(authentication, :kill)
  end

  defp assert_order_independent_connection_loss(order) do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        result =
          ConnectionGuard.run(
            connection,
            authentication,
            fn resource, reason ->
              send(test_process, {:classified, resource, reason})
              {:error, :server_unreachable}
            end,
            fn ->
              send(test_process, {:guard_worker, self()})
              receive do: (:continue -> :ok)
            end
          )

        send(test_process, {:guard_result, result})
      end)

    assert_receive {:guard_worker, worker}
    :erlang.suspend_process(owner)

    on_exit(fn ->
      if Process.alive?(owner), do: :erlang.resume_process(owner)
      Process.exit(owner, :kill)
      Process.exit(connection, :kill)
      Process.exit(authentication, :kill)
    end)

    case order do
      :connection_first ->
        stop_and_wait(connection, :shutdown)
        stop_and_wait(worker, :kill)

      :worker_first ->
        stop_and_wait(worker, :kill)
        stop_and_wait(connection, :shutdown)
    end

    :erlang.resume_process(owner)

    assert_receive {:classified, :connection, _bounded_reason}
    assert_receive {:guard_result, {:error, :server_unreachable}}
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}

    Process.exit(authentication, :kill)
  end

  defp idle_process do
    spawn(fn -> receive do: (:stop -> :ok) end)
  end

  defp assert_unknown_outcome_cleanup(failed_resource) do
    test_process = self()
    connection = idle_process()
    authentication = idle_process()

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        ConnectionGuard.run_unknown_outcome([connection, authentication], fn ->
          send(test_process, :unknown_outcome_guard_ready)
          receive do: (:continue -> :ok)
        end)
      end)

    assert_receive :unknown_outcome_guard_ready

    {failed, survivor} =
      case failed_resource do
        :authentication -> {authentication, connection}
        :connection -> {connection, authentication}
      end

    failed_monitor = Process.monitor(failed)
    survivor_monitor = Process.monitor(survivor)
    Process.exit(failed, :shutdown)

    assert_receive {:DOWN, ^failed_monitor, :process, ^failed, :shutdown}
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}
    assert_receive {:DOWN, ^survivor_monitor, :process, ^survivor, :killed}
  end

  defp stop_and_wait(process, reason) do
    monitor = Process.monitor(process)
    Process.exit(process, reason)
    expected_reason = if reason == :kill, do: :killed, else: reason
    assert_receive {:DOWN, ^monitor, :process, ^process, ^expected_reason}
  end
end
