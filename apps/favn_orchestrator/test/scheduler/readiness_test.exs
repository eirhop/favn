defmodule FavnOrchestrator.Scheduler.ReadinessTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Scheduler.Readiness

  test "allows startup grace and rejects stale or failed ticks" do
    now = DateTime.utc_now()

    assert :ok =
             Readiness.check(
               %{
                 auto_tick?: true,
                 tick_ms: 1_000,
                 started_at: DateTime.add(now, -2, :second),
                 last_tick_at: nil,
                 last_error: nil
               },
               now
             )

    assert {:error, :scheduler_tick_stale} =
             Readiness.check(
               %{
                 auto_tick?: true,
                 tick_ms: 1_000,
                 started_at: DateTime.add(now, -6, :second),
                 last_tick_at: nil,
                 last_error: nil
               },
               now
             )

    assert {:error, :scheduler_tick_failed} =
             Readiness.check(
               %{
                 auto_tick?: true,
                 tick_ms: 1_000,
                 started_at: now,
                 last_tick_at: now,
                 last_error: :database_unavailable
               },
               now
             )
  end
end
