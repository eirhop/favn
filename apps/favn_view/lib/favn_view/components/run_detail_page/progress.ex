defmodule FavnView.Components.RunDetailPage.Progress do
  @moduledoc """
  How far a run got, as one meter per population that actually has parts.

  This replaces a row of seven counters. Four of those counters were a
  decomposition of a fifth, so the reader had to add them up to see the whole,
  and every outcome that had not happened still spent a card saying zero. A meter
  shows the whole first and omits what did not occur.

  The window meter appears only for a run with more than one window. A single
  window run has nothing to say about window progress that the asset meter has
  not already said.
  """

  use FavnView, :html

  attr :run, :map, required: true
  attr :class, :any, default: nil

  def run_progress(assigns) do
    assigns =
      assigns
      |> assign(:assets, asset_segments(assigns.run))
      |> assign(:windows, window_segments(assigns.run))
      |> assign(:asset_summary, summary(assigns.run.total_asset_attempts, "asset step"))
      |> assign(:window_summary, summary(assigns.run.total_windows, "window"))

    ~H"""
    <section
      class={["grid gap-3 sm:grid-cols-2 sm:gap-6", @class]}
      data-testid="execution-group-header"
    >
      <span class="sr-only" data-testid="execution-group-id">{@run.id}</span>
      <.outcome_meter segments={@assets} summary={@asset_summary} data-testid="asset-progress" />
      <.outcome_meter
        :if={@windows != []}
        segments={@windows}
        summary={@window_summary}
        data-testid="window-progress"
      />
    </section>
    """
  end

  defp asset_segments(run) do
    [
      %{tone: :success, count: run.succeeded_asset_attempts, label: "ran"},
      %{tone: :neutral, count: run.skipped_asset_attempts, label: "already fresh"},
      %{tone: :error, count: run.failed_asset_attempts, label: "failed"},
      %{tone: :info, count: run.running_asset_attempts, label: "running"},
      %{tone: :warning, count: run.queued_asset_attempts, label: "queued"},
      %{tone: :neutral, count: run.planned_asset_attempts, label: "planned"}
    ]
  end

  defp window_segments(%{total_windows: total} = run) when is_integer(total) and total > 1 do
    cancelled = Map.get(run, :cancelled_windows, 0)
    running = Map.get(run, :running_windows, 0)
    queued = Map.get(run, :queued_windows, 0)
    succeeded = max(run.completed_windows - run.failed_windows - cancelled, 0)
    remaining = max(total - succeeded - run.failed_windows - cancelled - running - queued, 0)

    [
      %{tone: :success, count: succeeded, label: "complete"},
      %{tone: :error, count: run.failed_windows, label: "failed"},
      %{tone: :neutral, count: cancelled, label: "cancelled"},
      %{tone: :info, count: running, label: "running"},
      %{tone: :warning, count: queued, label: "queued"},
      %{tone: :neutral, count: remaining, label: "remaining"}
    ]
  end

  defp window_segments(_run), do: []

  defp summary(1, noun), do: "1 #{noun}"
  defp summary(count, noun) when is_integer(count), do: "#{count} #{noun}s"
  defp summary(_count, noun), do: "#{noun}s"
end
