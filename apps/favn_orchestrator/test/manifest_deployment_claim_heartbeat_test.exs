defmodule FavnOrchestrator.ManifestDeploymentClaimHeartbeatTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestDeploymentClaimHeartbeat

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
