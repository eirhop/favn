defmodule FavnOrchestrator.ManifestDeploymentClaimHeartbeatTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.ManifestDeploymentClaimHeartbeat

  test "cleanup diagnostics log transitions and coalesce repeated stuck warnings" do
    previous_level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous_level) end)
    ref = make_ref()

    batch = %{
      workspace_id: "workspace",
      operation_id: "operation",
      cleanup_state: "unknown",
      counts: %{"unknown" => 1},
      task_ids: []
    }

    state = %{reconciliation: ref, cleanup_diagnostics: %{}}

    first =
      ExUnit.CaptureLog.capture_log([level: :info], fn ->
        assert {:noreply, _} =
                 FavnOrchestrator.ManifestDeploymentDispatcher.handle_info(
                   {ref, {:ok, [batch]}},
                   state
                 )
      end)

    assert first =~ "deployment inspection cleanup unknown"
    now = System.monotonic_time(:second)
    repeat = %{state | cleanup_diagnostics: %{{"workspace", "operation"} => {"unknown", now}}}

    assert ExUnit.CaptureLog.capture_log([level: :info], fn ->
             FavnOrchestrator.ManifestDeploymentDispatcher.handle_info(
               {ref, {:ok, [batch]}},
               repeat
             )
           end) == ""

    overdue = %{
      state
      | cleanup_diagnostics: %{{"workspace", "operation"} => {"unknown", now - 61}}
    }

    assert ExUnit.CaptureLog.capture_log([level: :info], fn ->
             FavnOrchestrator.ManifestDeploymentDispatcher.handle_info(
               {ref, {:ok, [batch]}},
               overdue
             )
           end) =~ "deployment inspection cleanup remains unknown"
  end

  test "worker completion does not start another periodic polling chain" do
    ref = make_ref()
    state = %{active: %{ref => :operation}}

    assert {:noreply, %{active: %{}}} =
             FavnOrchestrator.ManifestDeploymentDispatcher.handle_info({ref, :ok}, state)

    refute_receive :poll, 0
  end

  test "stops with its deployment worker" do
    test_pid = self()

    worker =
      spawn(fn ->
        heartbeat =
          ManifestDeploymentClaimHeartbeat.start(
            fn ->
              send(test_pid, :claim_renewed)
              :ok
            end,
            interval_ms: 5
          )

        send(test_pid, {:heartbeat, heartbeat})
        Process.sleep(:infinity)
      end)

    assert_receive {:heartbeat, heartbeat}
    assert_receive :claim_renewed, 100
    heartbeat_monitor = Process.monitor(heartbeat.pid)

    Process.exit(worker, :kill)

    assert_receive {:DOWN, ^heartbeat_monitor, :process, _pid, :normal}, 100
  end

  test "terminates the worker when claim renewal is lost" do
    test_pid = self()

    worker =
      spawn(fn ->
        ManifestDeploymentClaimHeartbeat.start(
          fn -> {:error, :fenced} end,
          interval_ms: 5
        )

        send(test_pid, :heartbeat_started)
        Process.sleep(:infinity)
      end)

    worker_monitor = Process.monitor(worker)

    assert_receive :heartbeat_started

    assert_receive {:DOWN, ^worker_monitor, :process, ^worker,
                    {:manifest_deployment_claim_lost, :fenced}},
                   100
  end
end
