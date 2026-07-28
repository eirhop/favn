defmodule CrmDemo.Lifecycle.ScheduleProbe do
  @moduledoc """
  Reports the window the scheduler picked, so schedule anchors are observable.

  Run it from a schedule and compare the returned window with the occurrence
  time to see `anchor: :previous_complete_period` at work.
  """

  use Favn.Asset

  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  freshness(:always)
  meta(tags: [:lifecycle])

  @doc "Returns the scheduler-selected daily window."
  def asset(ctx) do
    {:ok,
     %{
       window_start: DateTime.to_iso8601(ctx.window.start_at),
       window_end: DateTime.to_iso8601(ctx.window.end_at)
     }}
  end
end
