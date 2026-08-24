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
end
