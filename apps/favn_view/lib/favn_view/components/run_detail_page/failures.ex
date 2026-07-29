defmodule FavnView.Components.RunDetailPage.Failures do
  @moduledoc """
  Failed window runs, called out above the flow.

  A failed asset attempt shows in its own lane, so it needs no callout. A window
  run that failed *before* producing any attempt has no lane to appear in — an
  admission failure, a fenced write, a runner that never picked the work up — and
  without this it would be invisible on a page whose whole job is to say what went
  wrong.
  """

  use FavnView, :html

  attr :run, :map, required: true

  def window_failures(assigns) do
    assigns =
      assigns
      |> assign(:failures, Map.get(assigns.run, :backfill_failures, []))
      |> assign(:total, failure_count(assigns.run))

    ~H"""
    <.notice
      :if={@failures != []}
      tone={:error}
      icon="hero-exclamation-triangle"
      data-testid="window-failures"
    >
      <p class="font-medium">
        {@total} window {if(@total == 1, do: "run", else: "runs")} failed without running assets
      </p>
      <ul class="mt-2 space-y-1">
        <li :for={failure <- @failures} class="text-xs" data-testid="window-failure-row">
          <span class="font-medium">{failure.window_label}</span>
          <span class="favn-text-muted">— {failure.error_summary}</span>
          <.link
            :if={failure.child_run_id}
            navigate={~p"/runs/#{failure.child_run_id}"}
            class="ml-1 font-medium underline-offset-2 hover:underline"
          >
            Open
          </.link>
        </li>
      </ul>
      <p :if={@total > length(@failures)} class="mt-2 text-xs favn-text-muted">
        Showing {length(@failures)} of {@total}.
      </p>
    </.notice>
    """
  end

  defp failure_count(run),
    do: Map.get(run, :backfill_failure_count, length(Map.get(run, :backfill_failures, [])))
end
