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
      class="mx-auto w-full max-w-6xl p-5 sm:p-6 lg:p-7"
      data-testid="run-overview-panel"
      data-run-active={to_string(@run.active?)}
    >
      <div class="flex flex-col gap-7">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-center">
          <div class="min-w-[10rem]">
            <p class="text-sm font-semibold text-base-content">Run status</p>
            <div class={["mt-3 flex items-center gap-3", status_text_class(@run.status_tone)]}>
              <.icon name={status_icon(@run.status_tone)} class="size-7" />
              <p class="text-2xl font-medium tracking-tight">{@run.status}</p>
            </div>
          </div>
          <p class="text-sm text-base-content/65">{status_sentence(@run.status)}</p>
        </div>

        <div
          :if={@run.failure_summary}
          class="rounded-box border border-error/25 bg-error/10 p-4 text-sm text-base-content/80"
          data-testid="run-failure-summary"
        >
          <p
            :if={@run.failure_summary.count > 0 && @run.failure_summary.total > 0}
            class="font-medium text-error"
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
          :if={@run.current_activity}
          class="rounded-box border border-info/20 bg-info/10 p-4 text-sm text-info-content/80"
          data-testid="run-current-activity"
        >
          {@run.current_activity}
        </div>

        <div class="border-t border-base-content/10 pt-5" data-testid="run-asset-results">
          <div class="grid grid-cols-[1fr_10rem_8rem_10rem_2rem] gap-4 px-4 pb-3 text-sm text-base-content/60 max-lg:hidden">
            <span>Asset</span>
            <span>Status</span>
            <span>Duration</span>
            <span>Started</span>
            <span class="sr-only">Inspect</span>
          </div>

          <div
            :if={@run.asset_results == []}
            class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
            data-testid="run-asset-results-empty"
          >
            {@run.asset_empty_message}
          </div>

          <div :if={@run.asset_results != []} class="space-y-2">
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
    <div
      class={[
        "group grid gap-3 rounded-box border bg-base-content/[0.025] p-4 transition hover:border-primary/35 hover:bg-primary/[0.045] hover:shadow-lg hover:shadow-primary/10 lg:grid-cols-[1fr_10rem_8rem_10rem_2rem] lg:items-center",
        row_border_class(@asset.status_tone)
      ]}
      data-testid="run-asset-result-row"
      data-run-id={@run_id}
      data-asset-step-id={@asset.id}
      role={if(@asset.inspectable?, do: "button", else: nil)}
      tabindex={if(@asset.inspectable?, do: "0", else: nil)}
    >
      <div class="flex min-w-0 items-center gap-3">
        <span class={[
          "flex size-9 shrink-0 items-center justify-center rounded-box",
          icon_shell_class(@asset.status_tone)
        ]}>
          <.icon name="hero-table-cells" class="size-5" />
        </span>
        <div class="min-w-0">
          <p class="truncate text-sm font-medium text-base-content">{@asset.display_name}</p>
          <p class="truncate font-mono text-xs text-base-content/45">{@asset.asset_ref}</p>
          <p :if={@asset.secondary} class="text-xs text-base-content/50">{@asset.secondary}</p>
          <p :if={@asset.error} class="mt-1 text-xs text-error">{@asset.error}</p>
        </div>
      </div>

      <div class="flex items-center gap-2 text-sm font-medium">
        <.icon
          name={status_icon(@asset.status_tone)}
          class={["size-4", status_text_class(@asset.status_tone)]}
        />
        <span class={status_text_class(@asset.status_tone)}>{@asset.status}</span>
      </div>

      <p class="text-sm text-base-content/85">{@asset.duration}</p>
      <p class="text-sm text-base-content/70">{@asset.started_at}</p>
      <%!-- TODO: Attach /runs/:run_id/assets/:asset_step_id navigation here once that route and ID contract exist. --%>
      <.icon
        :if={@asset.inspectable?}
        name="hero-chevron-right"
        class="size-5 text-base-content/60 transition group-hover:text-primary"
      />
    </div>
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
end
