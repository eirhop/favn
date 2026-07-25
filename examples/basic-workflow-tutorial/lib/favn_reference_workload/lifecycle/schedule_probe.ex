defmodule FavnReferenceWorkload.Lifecycle.ScheduleProbe do
  @moduledoc """
  Lightweight daily-window asset used to observe scheduler-created CLI runs.

  The asset has no data-plane side effects. Its returned metadata records the
  exact window selected by the scheduler.
  """

  use Favn.Asset

  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  freshness(:always)
  meta(category: :lifecycle_probe, tags: [:cli_qa, :schedule, :daily])

  @doc "Return the scheduler-selected daily window as run metadata."
  def asset(ctx) do
    {:ok,
     %{
       window_start: DateTime.to_iso8601(ctx.window.start_at),
       window_end: DateTime.to_iso8601(ctx.window.end_at),
       result: "scheduled"
     }}
  end
end
