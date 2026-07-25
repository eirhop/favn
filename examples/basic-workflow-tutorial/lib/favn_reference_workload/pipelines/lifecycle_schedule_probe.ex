defmodule FavnReferenceWorkload.Pipelines.LifecycleScheduleProbe do
  @moduledoc """
  Fast scheduler probe for CLI-only lifecycle testing.

  The six-field cron fires every ten seconds, but scheduler execution is opt-in:
  start the local stack with `mix favn.dev --scheduler` to activate it.
  """

  use Favn.Pipeline

  alias FavnReferenceWorkload.Lifecycle.ScheduleProbe

  pipeline :lifecycle_schedule_probe do
    asset(ScheduleProbe)
    deps(:all)
    window(:daily, anchor: :previous_complete_period, timezone: "Etc/UTC")

    schedule(
      cron: "*/10 * * * * *",
      timezone: "Etc/UTC",
      missed: :skip,
      overlap: :forbid
    )

    meta(%{purpose: "cli_schedule_qa"})
  end
end
