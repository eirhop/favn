defmodule FavnView.Components.RunOverviewHud do
  @moduledoc """
  Calm operator-facing overview HUD for a single run.
  """

  use FavnView, :html

  alias FavnView.Components.GlassPanel

  attr :run, :map, required: true

  def run_overview_hud(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      id="run-overview-panel"
      phx-hook="FavnClipboard"
      class="mx-auto w-full max-w-[96rem] p-4 sm:p-5 lg:p-6"
      data-testid="run-overview-panel"
      data-run-active={to_string(@run.active?)}
    >
      <div class="flex flex-col gap-5">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-center">
          <div class="min-w-[10rem]">
            <p class="text-xs font-semibold text-base-content">Run status</p>
            <div class={["mt-2 flex items-center gap-2.5", status_text_class(@run.status_tone)]}>
              <.icon name={status_icon(@run.status_tone)} class="size-6" />
              <p class="text-xl font-medium tracking-tight">{@run.status}</p>
            </div>
          </div>
          <p class="text-xs text-base-content/65 sm:text-sm">{status_sentence(@run.status)}</p>
        </div>

        <div
          :if={@run.failure_summary}
          class="rounded-box border border-error/25 bg-error/10 p-4 text-xs text-base-content/80"
          data-testid="run-failure-summary"
        >
          <div class="flex items-start justify-between gap-3">
            <p class="font-medium text-error">Failure details</p>
            <button
              :if={@run.failure_summary.error}
              type="button"
              class="btn btn-error btn-soft btn-xs rounded-box"
              data-copy-text={@run.failure_summary.error}
              data-testid="run-error-copy-button"
            >
              <.icon name="hero-clipboard-document" class="size-4" /> Copy
            </button>
          </div>
          <p
            :if={@run.failure_summary.kind == :backfill && @run.failure_summary.count > 0}
            class="mt-2 font-medium text-error"
          >
            {@run.failure_summary.count} backfill {if(@run.failure_summary.count == 1,
              do: "window",
              else: "windows"
            )} failed.
          </p>
          <p
            :if={
              @run.failure_summary.kind != :backfill && @run.failure_summary.count > 0 &&
                @run.failure_summary.total > 0
            }
            class="mt-2 font-medium text-error"
          >
            {@run.failure_summary.count} of {@run.failure_summary.total} assets failed.
          </p>
          <p :if={@run.failure_summary.asset} class="mt-1">
            <span class="text-base-content/55">Failed asset:</span> {@run.failure_summary.asset}
          </p>
          <p :if={@run.failure_summary.error} class="mt-1">
            <span class="text-base-content/55">Error:</span> {@run.failure_summary.error}
          </p>
        </div>

        <div
          :if={@run.backfill_failures != []}
          class="rounded-box border border-error/30 bg-error/10 p-4 text-xs text-base-content/85 shadow-lg shadow-error/10"
          data-testid="backfill-failure-list"
        >
          <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <p class="font-medium text-error">Failed backfill window</p>
              <p class="mt-1 text-base-content/60">
                Showing {length(@run.backfill_failures)} of {@run.backfill_failure_count} failed {if(
                  @run.backfill_failure_count == 1,
                  do: "window",
                  else: "windows"
                )}. Open a window run for the actionable execution context.
              </p>
            </div>
          </div>

          <div class="mt-3 max-h-64 space-y-2 overflow-y-auto pr-1">
            <div
              :for={failure <- @run.backfill_failures}
              class="rounded-box border border-error/20 bg-base-300/25 p-3"
              data-testid="backfill-failure-row"
            >
              <div class="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
                <div class="min-w-0 space-y-1">
                  <p class="font-mono text-xs font-medium text-base-content">
                    {failure.asset_ref || "Failed window run"}
                  </p>
                  <p class="text-base-content/60">
                    <span class="text-base-content/45">Window:</span> {failure.window}
                  </p>
                  <p :if={failure.error} class="text-error">
                    {failure.error}
                  </p>
                </div>

                <div class="flex shrink-0 flex-wrap items-center gap-2 text-base-content/60">
                  <span class="badge badge-error badge-soft badge-sm">{failure.status}</span>
                  <span :if={failure.attempt_count}>Attempt {failure.attempt_count}</span>
                  <span :if={failure.duration != "-"}>{failure.duration}</span>
                  <.link
                    :if={failure.child_run_href}
                    navigate={failure.child_run_href}
                    class="btn btn-error btn-soft btn-xs rounded-box"
                    data-testid="backfill-child-run-link"
                  >
                    Open window run
                  </.link>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div
          :if={@run.current_activity}
          class="rounded-box border border-info/35 bg-info/15 p-3 text-xs text-base-content/85 shadow-sm shadow-info/10"
          data-testid="run-current-activity"
        >
          {@run.current_activity}
        </div>

        <div class="border-t border-base-content/10 pt-4" data-testid="run-asset-results">
          <div class="grid grid-cols-[minmax(0,1fr)_8rem_6rem_9rem_1.5rem] gap-3 px-3 pb-2 text-xs text-base-content/60 max-lg:hidden">
            <span>Asset</span>
            <span>Status</span>
            <span>Duration</span>
            <span>Started</span>
            <span class="sr-only">Inspect</span>
          </div>

          <div
            :if={@run.asset_results == []}
            class="rounded-box border border-dashed border-base-content/15 p-4 text-xs text-base-content/55"
            data-testid="run-asset-results-empty"
          >
            {@run.asset_empty_message}
          </div>

          <div :if={@run.asset_results != []} class="space-y-1.5">
            <.asset_result_row :for={asset <- @run.asset_results} asset={asset} run_id={@run.id} />
          </div>
        </div>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :asset, :map, required: true
  attr :run_id, :string, required: true

  def asset_result_row(assigns) do
    ~H"""
    <.link
      :if={@asset.inspectable? && !@asset.error}
      navigate={~p"/runs/#{@run_id}/assets/#{@asset.id}/logs"}
      class={row_class(@asset, :link)}
      data-testid="run-asset-result-row"
      data-run-id={@run_id}
      data-asset-step-id={@asset.id}
    >
      <.asset_result_row_content asset={@asset} />
    </.link>

    <div
      :if={@asset.inspectable? && @asset.error}
      class={row_class(@asset, :static)}
      data-testid="run-asset-result-row"
      data-run-id={@run_id}
      data-asset-step-id={@asset.id}
    >
      <.asset_result_row_content asset={@asset} />
      <div class="flex flex-wrap gap-2 lg:col-span-5 lg:col-start-1">
        <button
          type="button"
          class="btn btn-error btn-soft btn-xs rounded-box"
          data-copy-text={@asset.error}
          data-testid="asset-error-copy-button"
        >
          <.icon name="hero-clipboard-document" class="size-4" /> Copy error
        </button>
        <.link
          navigate={~p"/runs/#{@run_id}/assets/#{@asset.id}/logs"}
          class="btn btn-ghost btn-xs rounded-box"
          data-testid="asset-logs-link"
        >
          View logs
        </.link>
      </div>
    </div>

    <div
      :if={!@asset.inspectable?}
      class={row_class(@asset, :static)}
      data-testid="run-asset-result-row"
      data-run-id={@run_id}
      data-asset-step-id={@asset.id}
    >
      <.asset_result_row_content asset={@asset} />
    </div>
    """
  end

  attr :asset, :map, required: true

  def asset_result_row_content(assigns) do
    ~H"""
    <div class="flex min-w-0 items-center gap-2.5">
      <span class={[
        "flex size-8 shrink-0 items-center justify-center rounded-box",
        icon_shell_class(@asset.status_tone)
      ]}>
        <.icon name="hero-table-cells" class="size-4" />
      </span>
      <div class="min-w-0">
        <p class="truncate text-xs font-medium text-base-content">{@asset.display_name}</p>
        <p class="truncate font-mono text-xs text-base-content/45">{@asset.asset_ref}</p>
        <p :if={@asset.secondary} class="text-xs text-base-content/50">{@asset.secondary}</p>
        <p :if={@asset.explanation} class="mt-1 text-xs text-base-content/60">{@asset.explanation}</p>
        <p :if={@asset.error} class="mt-1 text-xs text-error">{@asset.error}</p>
      </div>
    </div>

    <div class="flex items-center gap-2 text-xs font-medium">
      <.icon
        name={status_icon(@asset.status_tone)}
        class={["size-4", status_text_class(@asset.status_tone)]}
      />
      <span class={status_text_class(@asset.status_tone)}>{@asset.status}</span>
    </div>

    <p class="text-xs text-base-content/85">{@asset.duration}</p>
    <p class="text-xs text-base-content/70">{@asset.started_at}</p>
    <.icon
      :if={@asset.inspectable?}
      name="hero-chevron-right"
      class="size-4 text-base-content/60 transition group-hover:text-primary"
    />
    """
  end

  defp status_sentence("Succeeded"), do: "All selected assets completed successfully."
  defp status_sentence("Running"), do: "Run is currently executing."
  defp status_sentence("Pending"), do: "Run is queued for execution."
  defp status_sentence("Failed"), do: "One or more assets failed."
  defp status_sentence("Partial"), do: "Run completed with partial results."
  defp status_sentence("Cancelled"), do: "Run was cancelled."
  defp status_sentence("Timed out"), do: "One or more assets timed out."
  defp status_sentence(_status), do: "Run status is not known yet."

  defp status_icon(:success), do: "hero-check-circle"
  defp status_icon(:info), do: "hero-arrow-path"
  defp status_icon(:warning), do: "hero-exclamation-triangle"
  defp status_icon(:error), do: "hero-x-circle"
  defp status_icon(_tone), do: "hero-minus-circle"

  defp status_text_class(:success), do: "text-success"
  defp status_text_class(:info), do: "text-info"
  defp status_text_class(:warning), do: "text-warning"
  defp status_text_class(:error), do: "text-error"
  defp status_text_class(_tone), do: "text-base-content/60"

  defp icon_shell_class(:success), do: "bg-success/15 text-success"
  defp icon_shell_class(:info), do: "bg-info/15 text-info"
  defp icon_shell_class(:warning), do: "bg-warning/15 text-warning"
  defp icon_shell_class(:error), do: "bg-error/15 text-error"
  defp icon_shell_class(_tone), do: "bg-base-content/10 text-base-content/60"

  defp row_border_class(:error), do: "border-error/40 shadow-error/10 shadow-lg"
  defp row_border_class(_tone), do: "border-base-content/10"

  defp row_class(asset, :link) do
    [
      "group grid gap-2.5 rounded-box border bg-base-content/[0.025] p-3 transition hover:border-primary/35 hover:bg-primary/[0.045] hover:shadow-lg hover:shadow-primary/10 lg:grid-cols-[minmax(0,1fr)_8rem_6rem_9rem_1.5rem] lg:items-center no-underline",
      row_border_class(asset.status_tone)
    ]
  end

  defp row_class(asset, :static) do
    [
      "group grid gap-2.5 rounded-box border bg-base-content/[0.025] p-3 transition lg:grid-cols-[minmax(0,1fr)_8rem_6rem_9rem_1.5rem] lg:items-center",
      row_border_class(asset.status_tone)
    ]
  end
end
