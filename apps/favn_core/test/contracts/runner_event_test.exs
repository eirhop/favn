defmodule Favn.Contracts.RunnerEventTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerEvent

  test "carries pinned manifest identity for emitted events" do
    now = ~U[2026-01-01 00:00:00Z]

    event =
      %RunnerEvent{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        event_type: :asset_started,
        occurred_at: now,
        payload: %{asset_ref: {MyApp.Asset, :asset}}
      }

    assert event.manifest_version_id == "mv_1"
    assert event.manifest_content_hash == "abc"
    assert event.event_type == :asset_started
    assert event.occurred_at == now
  end
end
