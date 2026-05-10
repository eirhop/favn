defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Read-only run detail page for operator inspection.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :active_mode, :atom, default: :overview

  def run_detail_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title={page_title(@run, @run_id)}
      subtitle={page_subtitle(@run)}
      status={@run[:status]}
      status_tone={@run[:status_tone] || :neutral}
      nav_items={@nav_items}
    >
      <.not_found_panel :if={!@run[:found?]} run={@run} />
      <.run_mode_panel :if={@run[:found?]} run={@run} active_mode={@active_mode} />

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={run_modes()} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :run, :map, required: true
  attr :active_mode, :atom, required: true

  def run_mode_panel(assigns) do
    ~H"""
    <.overview_panel :if={@active_mode == :overview} run={@run} />
    <.placeholder_panel
      :if={@active_mode == :events}
      title="Events"
      copy="Detailed event filtering will follow."
    />
    <.placeholder_panel
      :if={@active_mode == :assets}
      title="Assets"
      copy="Asset-level controls stay read-only for now."
    />
    <.placeholder_panel
      :if={@active_mode == :output}
      title="Output"
      copy="Output previews are not available yet."
    />
    <.placeholder_panel
      :if={@active_mode == :debug}
      title="Debug"
      copy="Debug data is intentionally hidden by default."
    />
    """
  end

  attr :run, :map, required: true

  def overview_panel(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-6xl flex-col gap-5" data-testid="run-overview-panel">
      <GlassPanel.glass_panel class="p-5 sm:p-6 lg:p-7">
        <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-5" data-testid="run-summary-row">
          <.summary_item label="Status" value={@run.status} tone={@run.status_tone} />
          <.summary_item label="Target" value={@run.target} />
          <.summary_item label="Started" value={@run.started_at} />
          <.summary_item label="Duration" value={@run.duration} />
          <.summary_item label="Manifest" value={@run.manifest_version_id} />
        </div>
      </GlassPanel.glass_panel>

      <GlassPanel.glass_panel class="p-5 sm:p-6 lg:p-7" data-testid="run-asset-results">
        <div class="mb-4 flex items-center justify-between gap-3">
          <div>
            <h2 class="text-lg font-medium tracking-tight">Asset execution</h2>
            <p class="text-sm text-base-content/55">
              Stage grouped when execution stage data exists.
            </p>
          </div>
          <span class="badge badge-ghost">{length(@run.asset_results)} assets</span>
        </div>

        <div
          :if={@run.asset_results == []}
          class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
        >
          No asset results persisted for this run yet.
        </div>

        <div :if={@run.asset_results != []} class="flex flex-col gap-4">
          <section
            :for={group <- @run.asset_result_groups}
            class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-3"
          >
            <h3 class="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-base-content/45">
              {stage_label(group.stage)}
            </h3>
            <div class="divide-y divide-base-content/10">
              <.asset_result_row :for={asset <- group.items} asset={asset} />
            </div>
          </section>
        </div>
      </GlassPanel.glass_panel>

      <GlassPanel.glass_panel class="p-5 sm:p-6 lg:p-7" data-testid="run-event-timeline">
        <div class="mb-4 flex items-center justify-between gap-3">
          <div>
            <h2 class="text-lg font-medium tracking-tight">Event timeline</h2>
            <p class="text-sm text-base-content/55">Latest persisted run events.</p>
          </div>
          <span class="badge badge-ghost">{length(@run.events)} events</span>
        </div>

        <div
          :if={@run.latest_events == []}
          class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
        >
          No events persisted for this run yet.
        </div>

        <ol :if={@run.latest_events != []} class="space-y-3">
          <.event_row :for={event <- @run.latest_events} event={event} />
        </ol>
      </GlassPanel.glass_panel>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :tone, :atom, default: nil

  def summary_item(assigns) do
    ~H"""
    <div class="rounded-box border border-base-content/10 bg-base-content/[0.035] p-3">
      <p class="text-xs uppercase tracking-[0.16em] text-base-content/45">{@label}</p>
      <p class="mt-1 truncate text-sm font-medium text-base-content" title={@value}>
        <span :if={@tone} class={["badge badge-soft", badge_class(@tone)]}>{@value}</span>
        <span :if={!@tone}>{@value}</span>
      </p>
    </div>
    """
  end

  attr :asset, :map, required: true

  def asset_result_row(assigns) do
    ~H"""
    <div class="flex flex-col gap-2 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div class="min-w-0">
        <p class="truncate text-sm font-medium text-base-content">{@asset.ref}</p>
        <p class="text-xs text-base-content/50">Started {@asset.started_at} · {@asset.duration}</p>
        <p :if={@asset.error} class="mt-1 text-xs text-error">{@asset.error}</p>
      </div>
      <span class={["badge badge-soft shrink-0", badge_class(@asset.status_tone)]}>
        {@asset.status}
      </span>
    </div>
    """
  end

  attr :event, :map, required: true

  def event_row(assigns) do
    ~H"""
    <li class="grid gap-2 rounded-box border border-base-content/10 bg-base-content/[0.03] p-3 sm:grid-cols-[10rem_12rem_1fr] sm:items-start">
      <time class="text-xs text-base-content/50">{@event.timestamp}</time>
      <div class="flex items-center gap-2">
        <span class="badge badge-ghost badge-sm">#{@event.sequence}</span>
        <span class="text-sm font-medium text-base-content">{@event.event_type}</span>
      </div>
      <p class="text-sm text-base-content/65">{@event.summary}</p>
    </li>
    """
  end

  attr :run, :map, required: true

  def not_found_panel(assigns) do
    ~H"""
    <div class="mx-auto w-full max-w-3xl">
      <GlassPanel.glass_panel class="p-8 text-center" data-testid="run-not-found-state">
        <h2 class="text-xl font-medium">{@run.error || "Run not found"}</h2>
        <p class="mt-2 text-sm text-base-content/60">
          No persisted run snapshot matches <span class="font-mono">{@run.id}</span>.
        </p>
        <.link navigate={~p"/assets"} class="btn btn-primary btn-soft mt-6">Back to assets</.link>
      </GlassPanel.glass_panel>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :copy, :string, required: true

  def placeholder_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto w-full max-w-3xl p-8 text-center"
      data-testid="run-mode-placeholder"
    >
      <h2 class="text-xl font-medium tracking-tight">{@title}</h2>
      <p class="mt-2 text-sm text-base-content/60">{@copy}</p>
    </GlassPanel.glass_panel>
    """
  end

  def sample_run(status \\ :running) do
    %{
      found?: true,
      id: "run_01jrun_detail_sample",
      short_id: "run_01jrun_detail",
      title: "Run run_01jrun_detail",
      subtitle: "FavnView.Assets.customer_orders_daily · Manual · window:day:2026-06-12",
      status: status_label(status),
      status_tone: status_tone(status),
      target: "FavnView.Assets.customer_orders_daily",
      trigger: "Manual",
      window: "window:day:2026-06-12",
      started_at: "Jun 12, 2026 14:00:00 UTC",
      finished_at: if(status == :running, do: "-", else: "Jun 12, 2026 14:00:04 UTC"),
      duration: if(status == :running, do: "4.2 s", else: "4.0 s"),
      manifest_version_id: "mv_customer_orders",
      asset_results: sample_asset_results(status),
      asset_result_groups:
        sample_asset_results(status)
        |> Enum.group_by(& &1.stage)
        |> Enum.map(fn {stage, items} -> %{stage: stage, items: items} end),
      events: sample_events(status),
      latest_events: sample_events(status)
    }
  end

  def empty_run do
    sample_run(:running)
    |> Map.put(:asset_results, [])
    |> Map.put(:asset_result_groups, [])
    |> Map.put(:events, [])
    |> Map.put(:latest_events, [])
  end

  def not_found_run do
    %{
      found?: false,
      id: "run_missing",
      error: "Run not found",
      status: nil,
      status_tone: :neutral
    }
  end

  def sample_nav_items, do: FavnView.Components.AssetCataloguePage.nav_items()

  defp sample_asset_results(:running) do
    [
      %{
        ref: "FavnView.Assets.customer_orders_daily",
        stage: 0,
        status: "Running",
        status_tone: :info,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "4.2 s",
        error: nil
      }
    ]
  end

  defp sample_asset_results(:error) do
    [
      %{
        ref: "FavnView.Assets.raw_orders",
        stage: 0,
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.3 s",
        error: nil
      },
      %{
        ref: "FavnView.Assets.customer_orders_daily",
        stage: 1,
        status: "Failed",
        status_tone: :error,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "2.1 s",
        error: "Warehouse timeout"
      }
    ]
  end

  defp sample_asset_results(_status) do
    [
      %{
        ref: "FavnView.Assets.raw_orders",
        stage: 0,
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.3 s",
        error: nil
      },
      %{
        ref: "FavnView.Assets.customer_orders_daily",
        stage: 1,
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "1.8 s",
        error: nil
      }
    ]
  end

  defp sample_events(:error) do
    [
      %{
        sequence: 1,
        timestamp: "Jun 12, 2026 14:00:00 UTC",
        event_type: "Run started",
        status: "Running",
        status_tone: :info,
        summary: "Run accepted by orchestrator"
      },
      %{
        sequence: 2,
        timestamp: "Jun 12, 2026 14:00:02 UTC",
        event_type: "Step failed",
        status: "Failed",
        status_tone: :error,
        summary: "Asset FavnView.Assets.customer_orders_daily"
      },
      %{
        sequence: 3,
        timestamp: "Jun 12, 2026 14:00:04 UTC",
        event_type: "Run failed",
        status: "Failed",
        status_tone: :error,
        summary: "Warehouse timeout"
      }
    ]
  end

  defp sample_events(status) do
    [
      %{
        sequence: 1,
        timestamp: "Jun 12, 2026 14:00:00 UTC",
        event_type: "Run started",
        status: "Running",
        status_tone: :info,
        summary: "Run accepted by orchestrator"
      },
      %{
        sequence: 2,
        timestamp: "Jun 12, 2026 14:00:04 UTC",
        event_type: if(status == :running, do: "Step started", else: "Run finished"),
        status: status_label(status),
        status_tone: status_tone(status),
        summary:
          if(status == :running,
            do: "Asset FavnView.Assets.customer_orders_daily",
            else: "All selected assets completed"
          )
      }
    ]
  end

  defp run_modes do
    [
      %{id: :overview, label: "Overview", icon: "hero-squares-2x2"},
      %{id: :events, label: "Events", icon: "hero-clock"},
      %{id: :assets, label: "Assets", icon: "hero-circle-stack"},
      %{id: :output, label: "Output", icon: "hero-document-text"},
      %{id: :debug, label: "Debug", icon: "hero-bug-ant"}
    ]
  end

  defp page_title(%{found?: true, title: title}, _run_id), do: title
  defp page_title(_run, run_id), do: "Run #{short_id(run_id)}"

  defp page_subtitle(%{found?: true, subtitle: subtitle}), do: subtitle
  defp page_subtitle(_run), do: "Run detail"

  defp stage_label(nil), do: "Stage unknown"
  defp stage_label(stage), do: "Stage #{stage}"

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:running), do: "Running"
  defp status_label(:error), do: "Failed"
  defp status_label(:partial), do: "Partial"

  defp status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp status_tone(:ok), do: :success
  defp status_tone(:running), do: :info
  defp status_tone(:error), do: :error
  defp status_tone(:partial), do: :warning
  defp status_tone(_status), do: :neutral

  defp badge_class(:success), do: "badge-success"
  defp badge_class(:info), do: "badge-info"
  defp badge_class(:warning), do: "badge-warning"
  defp badge_class(:error), do: "badge-error"
  defp badge_class(_tone), do: "badge-neutral"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
