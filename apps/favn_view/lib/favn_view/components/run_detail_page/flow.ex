defmodule FavnView.Components.RunDetailPage.Flow do
  @moduledoc """
  The bounded asset list for one exact run.

  Rows are the run's asset attempts, which appear from the moment a step is
  queued, and contain only the name, state, start, and end values shown here.
  Every row links to its separate detail route.

  The chart and the table draw the same rows. The chart is the default reading
  because a run is a shape in time; the table stays one click away for the exact
  values, and remains the only reading on a narrow screen, where the card list
  takes over from both.

  In compare mode the chart position is taken by the multi-window comparison.
  The status filter and sort controls belong to the single-run chart and are not
  shown there: a control that narrowed one window and not the others would make
  the comparison say something untrue.
  """

  use FavnView, :html

  alias FavnView.Components.RunDetailPage.Comparison
  alias FavnView.Components.RunDetailPage.Timeline

  attr :assets, :list, required: true
  attr :chart, :any, default: nil
  attr :comparison, :any, default: nil
  attr :view, :atom, values: [:chart, :table], default: :chart
  attr :filter, :list, default: []
  attr :sort, :atom, values: [:start, :name], default: :start
  attr :on_set_view, :string, default: "set_flow_view"
  attr :on_toggle_filter, :string, default: "toggle_flow_filter"
  attr :on_set_sort, :string, default: "set_flow_sort"
  attr :backfill_parent?, :boolean, default: false

  @outcomes [succeeded: "Succeeded", failed: "Failed", running: "Running", waiting: "Waiting"]
  @sorts [start: "Start", name: "Name"]

  def flow(assigns) do
    ~H"""
    <section data-testid="run-flow">
      <.empty_state
        :if={@assets == [] && !@backfill_parent?}
        icon="hero-square-3-stack-3d"
        title="No asset work yet"
        description="Assets appear as the run queues them for execution."
      />

      <.empty_state
        :if={@assets == [] && @backfill_parent?}
        icon="hero-calendar-days"
        title="Asset work runs in the windows"
        description="Open a window run to inspect its exact assets and results."
      />

      <.panel :if={@assets != []} padding={:none}>
        <:header title="Assets" />
        <:actions>
          <div
            :if={@chart}
            class="hidden lg:flex"
            role="group"
            aria-label="Asset view"
            data-testid="run-flow-view-toggle"
          >
            <button
              :for={{view, label} <- [chart: "Chart", table: "Table"]}
              type="button"
              phx-click={@on_set_view}
              phx-value-view={view}
              aria-pressed={to_string(@view == view)}
              data-testid="run-flow-view-option"
              class={[
                "border px-2 py-1 text-xs transition-colors first:rounded-l-md last:rounded-r-md",
                "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
                @view == view && "border-primary/60 bg-primary/10 font-medium",
                @view != view && "border-base-content/15 hover:bg-base-content/5"
              ]}
            >
              {label}
            </button>
          </div>
          <.badge tone={:neutral} variant={:outline}>{length(@assets)} assets</.badge>
        </:actions>

        <div :if={@comparison && @view == :chart} class="hidden lg:block">
          <Comparison.comparison chart={@comparison} />
        </div>

        <%!-- A comparison is several charts side by side, which a phone has no
        room for. Rather than quietly showing this window's own rows as if they
        were the comparison, the narrow layout says where the chart went. --%>
        <.notice
          :if={@comparison}
          tone={:info}
          icon="hero-computer-desktop"
          class="m-3 lg:hidden"
          data-testid="run-comparison-narrow"
        >
          Comparing windows needs a wider screen. This window's assets are below.
        </.notice>

        <div :if={is_nil(@comparison) && @chart && @view == :chart} class="hidden lg:block">
          <%!-- Both controls are view state. Neither issues a read: they narrow
          and reorder the rows the page already holds, which is what makes a run
          with hundreds of assets readable. --%>
          <div
            class="flex flex-wrap items-center gap-3 border-b border-base-content/10 px-3 py-2"
            data-testid="run-flow-controls"
          >
            <div class="flex flex-wrap gap-1" role="group" aria-label="Status filter">
              <button
                :for={{outcome, label} <- outcomes()}
                type="button"
                phx-click={@on_toggle_filter}
                phx-value-outcome={outcome}
                aria-pressed={to_string(outcome in @filter)}
                data-testid="run-flow-filter-chip"
                class={[
                  "rounded-md border px-2 py-1 text-xs transition-colors",
                  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
                  outcome in @filter && "border-primary/60 bg-primary/10 font-medium",
                  outcome not in @filter && "border-base-content/15 hover:bg-base-content/5"
                ]}
              >
                {label}
              </button>
            </div>

            <div class="flex items-center gap-1 text-xs favn-text-subtle">
              <span>Sort</span>
              <button
                :for={{sort, label} <- sorts()}
                type="button"
                phx-click={@on_set_sort}
                phx-value-sort={sort}
                aria-pressed={to_string(@sort == sort)}
                data-testid="run-flow-sort-option"
                class={[
                  "rounded-md border px-2 py-1 transition-colors",
                  "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary",
                  @sort == sort && "border-primary/60 bg-primary/10 font-medium",
                  @sort != sort && "border-base-content/15 hover:bg-base-content/5"
                ]}
              >
                {label}
              </button>
            </div>
          </div>

          <Timeline.timeline chart={@chart} />
        </div>

        <.data_table
          :if={is_nil(@chart) or @view == :table}
          id="run-assets"
          rows={@assets}
          row_testid="run-asset-row"
          desktop_only?
        >
          <:col :let={asset} label="Asset">
            <.link
              navigate={~p"/runs/#{asset.run_id}/assets/#{asset.id}"}
              class="font-medium link link-hover"
            >
              {asset.name}
            </.link>
            <.mono value={asset.asset_ref} truncate class="mt-0.5 text-sm favn-text-subtle" />
          </:col>
          <:col :let={asset} label="State" class="w-32">
            <.status_badge
              tone={status_tone(asset.state)}
              label={status_label(asset.state)}
              size={:sm}
            />
          </:col>
          <:col :let={asset} label="Started" class="w-44 favn-text-subtle">
            {asset.started_label || "-"}
          </:col>
          <:col :let={asset} label="Finished" class="w-44 favn-text-subtle">
            {asset.finished_label || "-"}
          </:col>
        </.data_table>

        <.stack gap={:sm} class="p-3 lg:hidden" data-testid="run-asset-card-list">
          <.asset_card :for={asset <- @assets} asset={asset} />
        </.stack>
      </.panel>
    </section>
    """
  end

  attr :asset, :map, required: true

  defp asset_card(assigns) do
    ~H"""
    <.list_card
      navigate={~p"/runs/#{@asset.run_id}/assets/#{@asset.id}"}
      data-testid="run-asset-card"
    >
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <.section_title>{@asset.name}</.section_title>
          <.mono value={@asset.asset_ref} truncate class="mt-1 text-sm favn-text-subtle" />
        </div>
        <.status_badge
          tone={status_tone(@asset.state)}
          label={status_label(@asset.state)}
          size={:sm}
        />
      </div>
      <.inline gap={:sm} class="mt-2 text-sm favn-text-subtle">
        <span>Started {@asset.started_label || "-"}</span>
        <span>Finished {@asset.finished_label || "-"}</span>
      </.inline>
    </.list_card>
    """
  end

  defp outcomes, do: @outcomes
  defp sorts, do: @sorts

  defp status_tone(status) when status in [:ok, :succeeded, :skipped_fresh], do: :success
  defp status_tone(status) when status in [:error, :failed, :timed_out, :blocked], do: :error
  defp status_tone(status) when status in [:running, :retrying], do: :info
  defp status_tone(status) when status in [:queued, :planned, :pending], do: :warning
  defp status_tone(_status), do: :neutral

  defp status_label(:skipped_fresh), do: "Skipped"

  defp status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()
end
