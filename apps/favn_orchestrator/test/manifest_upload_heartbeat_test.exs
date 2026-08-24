defmodule FavnOrchestrator.ManifestUploadHeartbeatTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ManifestUploadHeartbeat

  test "renews independently while the upload owner is busy" do
    owner = self()

    heartbeat =
      ManifestUploadHeartbeat.start(
        fn ->
          send(owner, :lease_renewed)
          :ok
        end,
        interval_ms: 5
      )

    assert_receive :lease_renewed, 100
    assert :ok = ManifestUploadHeartbeat.check(heartbeat)
    assert :ok = ManifestUploadHeartbeat.stop(heartbeat)
  end

  test "propagates lease loss to the upload owner" do
    heartbeat =
      ManifestUploadHeartbeat.start(fn -> {:error, :taken_over} end, interval_ms: 5)

    assert {:error, {:upload_lease_lost, :taken_over}} =
             ManifestUploadHeartbeat.check(heartbeat, 100)

    assert :ok = ManifestUploadHeartbeat.stop(heartbeat)
  end

  test "stops renewing when its upload owner dies" do
    test_pid = self()

    owner =
      spawn(fn ->
        heartbeat =
          ManifestUploadHeartbeat.start(
            fn ->
              send(test_pid, :lease_renewed)
              :ok
            end,
            interval_ms: 5
          )

        send(test_pid, {:heartbeat, heartbeat})
        Process.sleep(:infinity)
      end)

    assert_receive {:heartbeat, heartbeat}
    assert_receive :lease_renewed, 100
    heartbeat_monitor = Process.monitor(heartbeat.pid)

    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^heartbeat_monitor, :process, _pid, :normal}, 100
    flush_renewals()
    refute_receive :lease_renewed, 30
  end

  defp flush_renewals do
    receive do
      :lease_renewed -> flush_renewals()
    after
      0 -> :ok
    end
  end
end
