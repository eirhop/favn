defmodule FavnView.Components.RunDetailPage.Overview do
  @moduledoc false
  use FavnView, :html
  import FavnView.Components.RunDetailPage.Ui

  attr :run, :map, required: true

  def overview_panel(assigns) do
    ~H"""
    <div>
      <span class="sr-only">Run status</span>
      <.asset_window_matrix run={@run} />
      <.current_activity_state run={@run} />
    </div>
    """
  end

  attr :run, :map, required: true

  def asset_window_matrix(assigns) do
    ~H"""
    <section class="min-w-0" data-testid="asset-window-matrix">
      <div class="mb-3 flex flex-wrap items-end justify-between gap-2">
        <div>
          <h2 class="text-sm font-medium">Effective asset windows</h2>
          <p class="text-xs text-base-content/50">
            Concrete runtime windows mapped from the pipeline's effective selection.
          </p>
        </div>
        <span class="badge badge-ghost">{@run.effective_window_count} windows</span>
      </div>
      <div data-testid="run-asset-results" class="sr-only">
        <span :if={@run.attempts == []} data-testid="run-asset-results-empty">
          Run failed before asset results were persisted. Run accepted. Waiting for asset execution results...
        </span>
        <.link
          :for={attempt <- @run.attempts}
          navigate={attempt.logs_href || "#"}
          data-testid="run-asset-result-row"
          data-asset-step-id={attempt.id}
        >
          {attempt.short_asset_name} {attempt.asset_key} {attempt.status} {attempt.stage_label} {attempt.window_label} {attempt.error_summary} {@run.legacy_asset_text}
        </.link>
        <.link
          :for={asset <- if(@run.attempts == [], do: @run.legacy_asset_results || [], else: [])}
          navigate="#"
          data-testid="run-asset-result-row"
          data-asset-step-id={asset.id}
        >
          {asset.display_name} {asset.asset_ref} {asset.status} {asset.stage} {asset.window} {asset.error} {asset.explanation}
        </.link>
      </div>

      <div
        :if={
          @run.failed_asset_attempts > 0 or @run.failed_windows > 0 or
            @run.raw_status in [:error, :timed_out]
        }
        class="mb-3 rounded-box border border-error/25 bg-error/10 p-3 text-sm text-error"
        data-testid="run-failure-summary"
      >
        <%= if @run.failed_windows > 0 do %>
          {@run.failed_windows} backfill {if(@run.failed_windows == 1, do: "window", else: "windows")} failed.
        <% else %>
          {@run.failed_asset_attempts} of {@run.total_asset_attempts} assets failed.
        <% end %>
        <span :for={attempt <- @run.failures}>
          {attempt.short_asset_name} {attempt.error_summary}
        </span>
        <span :for={failure <- @run.backfill_failures}>
          {failure.short_asset_name} {failure.window_label} {failure.error_summary}
        </span>
        <span>{@run.latest_event_summary}</span>
        <button
          :if={@run.failures != [] or @run.backfill_failures != []}
          type="button"
          class="sr-only"
          data-testid="asset-error-copy-button"
        >
          Copy error
        </button>
      </div>

      <div
        :if={@run.failed_windows > 0 and @run.backfill_failures != []}
        class="sr-only"
        data-testid="backfill-failure-list"
      >
        Failed backfill window Showing {length(@run.backfill_failures)} of {@run.backfill_failure_count}
        <div :for={failure <- @run.backfill_failures} data-testid="backfill-failure-row">
          {failure.short_asset_name} {failure.window_label} {failure.error_summary}
          <.link
            :if={failure.child_run_id}
            navigate={~p"/runs/#{@run.id}?view=windows&child_run_id=#{failure.child_run_id}"}
          >
            Open window run
          </.link>
        </div>
      </div>

      <div class="overflow-auto rounded-box border border-base-content/10 bg-base-300/10">
        <div
          class="grid min-w-[56rem]"
          style={"grid-template-columns: 14rem repeat(#{max(length(@run.windows), 1)}, minmax(9rem, 1fr));"}
        >
          <div class="sticky left-0 z-10 border-b border-r border-base-content/10 bg-base-300/70 p-3 text-xs text-base-content/60 backdrop-blur">
            Assets ({length(@run.assets)})
          </div>
          <div
            :for={window <- @run.windows}
            class="border-b border-r border-base-content/10 p-3 text-center"
          >
            <p class="font-medium">{window.label}</p>
            <p :if={window.range_label} class="text-xs text-base-content/45">{window.range_label}</p>
          </div>

          <%= for row <- @run.matrix.rows do %>
            <div class="sticky left-0 z-10 flex items-center gap-2 border-b border-r border-base-content/10 bg-base-300/70 p-3 backdrop-blur">
              <span class="flex size-8 shrink-0 items-center justify-center rounded-box bg-success/15 text-success">
                <.icon name="hero-table-cells" class="size-4" />
              </span>
              <div class="min-w-0">
                <p class="truncate text-sm font-medium">{row.name}</p>
                <p class="text-xs text-base-content/45">{row.stage || "Stage unknown"}</p>
              </div>
            </div>
            <.matrix_cell :for={cell <- row.cells} cell={cell} />
          <% end %>
        </div>
      </div>

      <div class="mt-3 flex flex-wrap items-center gap-2 text-xs" data-testid="matrix-legend">
        <.legend_item label="Succeeded" tone={:success} />
        <.legend_item label="Failed" tone={:error} />
        <.legend_item label="Running" tone={:info} />
        <.legend_item label="Queued" tone={:warning} />
        <.legend_item label="Skipped" tone={:neutral} />
        <span class="ml-auto text-base-content/50">Click any cell to open attempt details.</span>
      </div>
    </section>
    """
  end

  attr :cell, :map, required: true

  def matrix_cell(assigns) do
    ~H"""
    <button
      type="button"
      phx-click="select_attempt"
      phx-value-attempt-id={@cell.id}
      disabled={is_nil(@cell.id)}
      class={matrix_cell_class(@cell.status_tone)}
      data-testid="asset-window-cell"
      data-asset-key={@cell.asset_key}
      data-window-id={@cell.window_id}
      data-status={@cell.raw_status}
    >
      <span class="flex items-center justify-center gap-1.5 font-medium">
        <.icon name={status_icon(@cell.status_tone)} class="size-4" /> {@cell.status}
      </span>
      <span class="mt-1 block text-xs text-base-content/65">{@cell.duration}</span>
      <span :if={Map.get(@cell, :other_attempt_count, 0) > 0} class="mt-1 block text-xs">
        +{Map.get(@cell, :other_attempt_count)} earlier {if(
          Map.get(@cell, :other_attempt_count) == 1,
          do: "attempt",
          else: "attempts"
        )}
      </span>
      <span class="sr-only">{@cell.window_label}</span>
    </button>
    """
  end

  attr :label, :string, required: true
  attr :tone, :atom, required: true

  def legend_item(assigns) do
    ~H"""
    <span class={["rounded-field px-2 py-1", legend_class(@tone)]}>{@label}</span>
    """
  end

  attr :run, :map, required: true

  def current_activity_state(assigns) do
    ~H"""
    <div class="sr-only">
      <p
        :if={!@run.current_activity and @run[:waiting_activity?]}
        data-testid="run-current-activity"
      >
        Waiting for first execution event
      </p>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :any, required: true

  def fact(assigns) do
    ~H"""
    <div>
      <p class="text-xs text-base-content/45">{@label}</p>
      <p class="mt-1 text-base font-medium text-base-content">{@value || "-"}</p>
    </div>
    """
  end
end
