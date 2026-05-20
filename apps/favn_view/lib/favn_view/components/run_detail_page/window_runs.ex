defmodule FavnView.Components.RunDetailPage.WindowRuns do
  @moduledoc false
  use FavnView, :html
  import FavnView.Components.RunDetailPage.Ui

  attr :run, :map, required: true

  def window_runs_panel(assigns) do
    ~H"""
    <section data-testid="window-runs-view">
      <h2 class="text-lg font-medium">Window runs</h2>
      <p class="text-sm text-base-content/55">Window runs created for this backfill.</p>

      <div class="mt-4 space-y-2">
        <div
          :for={child <- @run.child_runs}
          class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3"
          data-testid="window-run-row"
          data-run-id={child.id}
        >
          <div class="grid gap-3 lg:grid-cols-[minmax(0,1fr)_8rem_8rem_8rem_12rem] lg:items-center">
            <div class="min-w-0">
              <p class="font-medium">{child.window_label}</p>
              <.link navigate={~p"/runs/#{child.id}"} class="font-mono text-xs text-primary">
                {child.id}
              </.link>
            </div>
            <span class={status_badge_class(child.status_tone)}>{child.status}</span>
            <p class="text-sm">{child.progress}</p>
            <p class="text-sm">{child.duration}</p>
            <p class="text-xs text-base-content/55">
              {child.succeeded_count} succeeded · {child.failed_count} failed · {child.running_count} running
            </p>
          </div>
        </div>
      </div>
    </section>
    """
  end
end
