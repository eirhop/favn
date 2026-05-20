defmodule FavnView.Components.RunDetailPage.Failures do
  @moduledoc false
  use FavnView, :html

  attr :run, :map, required: true

  def failures_panel(assigns) do
    ~H"""
    <section data-testid="failures-view">
      <div class="mb-4 flex items-center justify-between gap-3">
        <div>
          <h2 class="text-lg font-medium">Failures</h2>
          <p class="text-sm text-base-content/55">
            Failed asset attempts with retry-relevant context.
          </p>
        </div>
        <span class="badge badge-error badge-soft">{length(@run.failures)} failed</span>
      </div>

      <div
        :if={@run.failures == []}
        class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
      >
        No failed asset attempts in this run.
      </div>

      <div :if={@run.failures != []} class="space-y-2">
        <.failure_row :for={attempt <- @run.failures} attempt={attempt} />
      </div>
    </section>
    """
  end

  attr :attempt, :map, required: true

  def failure_row(assigns) do
    ~H"""
    <button
      type="button"
      phx-click="select_attempt"
      phx-value-attempt-id={@attempt.id}
      class="grid w-full gap-3 rounded-box border border-error/30 bg-error/10 p-3 text-left transition hover:border-error/60 lg:grid-cols-[minmax(0,1fr)_8rem_10rem_6rem_8rem]"
      data-testid="failure-row"
    >
      <div class="min-w-0">
        <p class="truncate font-medium text-error">{@attempt.short_asset_name}</p>
        <p class="truncate text-xs text-base-content/55">
          {@attempt.error_summary || "No error summary"}
        </p>
      </div>
      <p class="text-sm">{@attempt.window_label}</p>
      <p class="font-mono text-xs text-base-content/60">{@attempt.child_run_id || @attempt.run_id}</p>
      <p class="text-sm">Attempt {@attempt.attempt_number || "-"}</p>
      <p class="text-sm">{@attempt.duration}</p>
    </button>
    """
  end
end
