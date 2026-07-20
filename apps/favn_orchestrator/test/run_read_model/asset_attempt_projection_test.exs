defmodule FavnOrchestrator.RunReadModel.AssetAttemptProjectionTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunReadModel.AssetAttemptProjection

  @occurred_at ~U[2026-07-20 10:05:00Z]

  test "preserves distinct effective windows that share one requested anchor" do
    first = event("step-one", ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])
    expanded = event("step-two", ~U[2026-06-24 00:00:00Z], ~U[2026-07-02 00:00:00Z])

    assert {:ok, first_attempt} = AssetAttemptProjection.from_event(first)
    assert {:ok, expanded_attempt} = AssetAttemptProjection.from_event(expanded)

    assert first_attempt.window.key == first_attempt.window_identity
    assert expanded_attempt.window.key == expanded_attempt.window_identity
    refute first_attempt.window_identity == expanded_attempt.window_identity
    assert first_attempt.window.start_at == ~U[2026-07-01 00:00:00Z]
    assert expanded_attempt.window.start_at == ~U[2026-06-24 00:00:00Z]
  end

  test "uses an explicit identity for non-windowed attempts" do
    event = %{
      event_type: :step_started,
      occurred_at: @occurred_at,
      asset_ref: {MyApp.Gold, :orders},
      data: %{asset_step_id: "step-none", attempt: 1}
    }

    assert {:ok, attempt} = AssetAttemptProjection.from_event(event)
    assert attempt.window_identity == "none"
    assert attempt.window == nil
    assert attempt.status == :running
    assert attempt.started_at == @occurred_at
  end

  test "ignores historical step events that do not carry a complete asset identity" do
    assert :ignore =
             AssetAttemptProjection.from_event(%{
               event_type: :step_started,
               occurred_at: @occurred_at,
               data: %{asset_step_id: "legacy-step"}
             })
  end

  test "projects the compact window carried by sequential completion events" do
    event = %{
      event_type: :step_finished,
      occurred_at: @occurred_at,
      asset_ref: {MyApp.Gold, :orders},
      data: %{
        asset_step_id: "sequential-step",
        attempt: 1,
        window: %{
          key: "requested:2026-07-01",
          kind: :day,
          start_at: ~U[2026-06-24 00:00:00Z],
          end_at: ~U[2026-07-02 00:00:00Z],
          timezone: "Etc/UTC"
        }
      }
    }

    assert {:ok, attempt} = AssetAttemptProjection.from_event(event)
    assert attempt.window.start_at == ~U[2026-06-24 00:00:00Z]
    assert String.starts_with?(attempt.window_identity, "runtime:")
  end

  defp event(asset_step_id, start_at, end_at) do
    %{
      event_type: :step_finished,
      occurred_at: @occurred_at,
      asset_ref: {MyApp.Gold, :orders},
      data: %{
        asset_step_id: asset_step_id,
        node_result: %{
          asset_step_id: asset_step_id,
          status: :ok,
          started_at: start_at,
          finished_at: @occurred_at,
          window: %{
            key: "requested:2026-07-01",
            kind: :day,
            start_at: start_at,
            end_at: end_at,
            timezone: "Etc/UTC"
          }
        }
      }
    }
  end
end
