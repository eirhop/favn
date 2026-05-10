defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Read-only run detail page for operator inspection.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail
  alias FavnView.Components.RunOverviewHud

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
      back_href={@run[:back_asset_href]}
      back_label={if(@run[:back_asset_href], do: "Back to asset", else: nil)}
      facts={run_facts(@run)}
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
    <RunOverviewHud.run_overview_hud :if={@active_mode == :overview} run={@run} />
    <.events_panel :if={@active_mode == :events} run={@run} />
    <.outputs_panel :if={@active_mode == :outputs} run={@run} />
    <.context_panel :if={@active_mode == :context} run={@run} />
    <.debug_panel :if={@active_mode == :debug} run={@run} />
    """
  end

  attr :run, :map, required: true

  def events_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel
      class="mx-auto w-full max-w-5xl p-5 sm:p-6 lg:p-7"
      data-testid="run-event-timeline"
    >
      <div class="mb-4 flex items-center justify-between gap-3">
        <div>
          <h2 class="text-lg font-medium tracking-tight">Events</h2>
          <p class="text-sm text-base-content/55">Chronological persisted run events.</p>
        </div>
        <span class="badge badge-ghost">{length(@run.events)} events</span>
      </div>

      <div
        :if={@run.events == []}
        class="rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
      >
        No events persisted for this run yet.
      </div>

      <ol :if={@run.events != []} class="space-y-3">
        <.event_row :for={event <- @run.events} event={event} />
      </ol>
    </GlassPanel.glass_panel>
    """
  end

  attr :event, :map, required: true

  def event_row(assigns) do
    ~H"""
    <li class="grid gap-2 rounded-box border border-base-content/10 bg-base-content/[0.03] p-3 sm:grid-cols-[5rem_11rem_12rem_1fr] sm:items-start">
      <span class="badge badge-ghost badge-sm">#{@event.sequence}</span>
      <time class="text-xs text-base-content/50">{@event.timestamp}</time>
      <span class="text-sm font-medium text-base-content">{@event.event_type}</span>
      <p class="text-sm text-base-content/65">
        <span :if={@event.asset} class="mr-2 font-mono text-xs text-base-content/45">
          {@event.asset}
        </span>
        {@event.summary}
      </p>
    </li>
    """
  end

  attr :run, :map, required: true

  def outputs_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto w-full max-w-4xl p-6" data-testid="run-outputs-panel">
      <h2 class="text-lg font-medium tracking-tight">Outputs</h2>
      <p class="mt-2 text-sm text-base-content/55">Persisted output metadata, when available.</p>

      <div
        :if={@run.outputs == []}
        class="mt-5 rounded-box border border-dashed border-base-content/15 p-5 text-sm text-base-content/55"
      >
        No output metadata persisted for this run yet.
      </div>

      <div :if={@run.outputs != []} class="mt-5 space-y-3">
        <div
          :for={output <- @run.outputs}
          class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
        >
          <p class="font-mono text-xs text-base-content/55">{output.asset}</p>
          <pre class="mt-2 overflow-auto text-xs text-base-content/75"><code>{output.output}</code></pre>
        </div>
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :run, :map, required: true

  def context_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto w-full max-w-4xl p-6" data-testid="run-context-panel">
      <h2 class="text-lg font-medium tracking-tight">Context</h2>
      <dl class="mt-5 grid gap-3 sm:grid-cols-2">
        <div
          :for={item <- @run.context}
          class="rounded-box border border-base-content/10 bg-base-content/[0.03] p-4"
        >
          <dt class="text-xs uppercase tracking-[0.16em] text-base-content/45">{item.label}</dt>
          <dd class="mt-1 break-words text-sm font-medium text-base-content">{item.value}</dd>
        </div>
      </dl>
    </GlassPanel.glass_panel>
    """
  end

  attr :run, :map, required: true

  def debug_panel(assigns) do
    ~H"""
    <GlassPanel.glass_panel class="mx-auto w-full max-w-5xl p-6" data-testid="run-debug-panel">
      <h2 class="text-lg font-medium tracking-tight">Debug</h2>
      <div class="mt-5 grid gap-4 lg:grid-cols-2">
        <.debug_block title="Run" value={@run.raw_run} />
        <.debug_block title="Events" value={@run.raw_events} />
      </div>
    </GlassPanel.glass_panel>
    """
  end

  attr :title, :string, required: true
  attr :value, :string, required: true

  def debug_block(assigns) do
    ~H"""
    <section class="min-w-0 rounded-box border border-base-content/10 bg-base-300/30 p-4">
      <h3 class="text-sm font-medium">{@title}</h3>
      <pre class="mt-3 max-h-[32rem] overflow-auto text-xs text-base-content/70"><code>{@value}</code></pre>
    </section>
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

  def sample_run(status \\ :running) do
    %{
      found?: true,
      id: "run_01jrun_detail_sample",
      raw_status: status,
      active?: status in [:pending, :running],
      short_id: "run_01jrun_detail",
      title: "run_01jrun_detail",
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
      events: sample_events(status),
      latest_event_summary: "All selected assets completed successfully.",
      current_activity: sample_current_activity(status),
      failure_summary: sample_failure_summary(status),
      asset_empty_message: sample_asset_empty_message(status),
      outputs: [],
      context: sample_context(),
      back_asset_href: "/assets/t-sample",
      raw_run: "%Favn.Run{...}",
      raw_events: "[%Favn.RunEvent{...}]"
    }
  end

  def empty_run do
    sample_run(:running)
    |> Map.put(:asset_results, [])
    |> Map.put(:events, [])
    |> Map.put(:latest_event_summary, nil)
    |> Map.put(:current_activity, "Waiting for first execution event...")
    |> Map.put(:asset_empty_message, "Run accepted. Waiting for asset execution results...")
  end

  def sample_run_with_no_results(status) do
    sample_run(status)
    |> Map.put(:asset_results, [])
    |> Map.put(:asset_empty_message, sample_asset_empty_message(status))
    |> Map.put(:current_activity, sample_current_activity(status))
    |> Map.put(:failure_summary, sample_no_result_failure_summary(status))
  end

  def sample_run_with_long_asset_names do
    sample_run(:ok)
    |> Map.put(:asset_results, [
      %{
        id: "long-asset-step",
        asset_ref:
          "FavnView.Assets.enterprise_revenue_operations.customer_orders_daily_with_extremely_long_asset_name_for_overflow_testing",
        display_name: "customer_orders_daily_with_extremely_long_asset_name_for_overflow_testing",
        secondary: "Stage 0",
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "12.4 s",
        error: nil,
        inspectable?: true
      }
    ])
  end

  def sample_run_with_node_statuses do
    sample_run(:partial)
    |> Map.put(:asset_results, [
      %{
        id: "node-raw-orders-window-2026-06-12",
        asset_ref: "FavnView.Assets.raw_orders",
        display_name: "raw_orders",
        secondary: "window:day:2026-06-12 · Fresh · Stage 0",
        status: "Skipped fresh",
        status_tone: :neutral,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.0 s",
        error: nil,
        inspectable?: true
      },
      %{
        id: "node-stg-orders-window-2026-06-12",
        asset_ref: "FavnView.Assets.stg_orders",
        display_name: "stg_orders",
        secondary: "window:day:2026-06-12 · Stage 1",
        status: "Retrying",
        status_tone: :info,
        started_at: "Jun 12, 2026 14:00:01 UTC",
        duration: "500 ms",
        error: nil,
        inspectable?: true
      },
      %{
        id: "node-customer-orders-window-2026-06-12",
        asset_ref: "FavnView.Assets.customer_orders_daily",
        display_name: "customer_orders_daily",
        secondary: "window:day:2026-06-12 · Upstream failed · Stage 2",
        status: "Blocked",
        status_tone: :error,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "1 ms",
        error: "Upstream failed",
        inspectable?: true
      }
    ])
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
        id: "run_01jrun_detail_sample-FavnView-Assets-customer_orders_daily",
        asset_ref: "FavnView.Assets.customer_orders_daily",
        display_name: "customer_orders_daily",
        secondary: "Stage 0",
        status: "Running",
        status_tone: :info,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "4.2 s",
        error: nil,
        inspectable?: true
      }
    ]
  end

  defp sample_asset_results(:partial) do
    [
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-raw_orders",
        asset_ref: "FavnView.Assets.raw_orders",
        display_name: "raw_orders",
        secondary: "Stage 0",
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.3 s",
        error: nil,
        inspectable?: true
      },
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-customer_orders_daily",
        asset_ref: "FavnView.Assets.customer_orders_daily",
        display_name: "customer_orders_daily",
        secondary: "Stage 1",
        status: "Failed",
        status_tone: :error,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "2.1 s",
        error: "Warehouse timeout",
        inspectable?: true
      }
    ]
  end

  defp sample_asset_results(:error) do
    [
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-raw_orders",
        asset_ref: "FavnView.Assets.raw_orders",
        display_name: "raw_orders",
        secondary: "Stage 0",
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.3 s",
        error: nil,
        inspectable?: true
      },
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-customer_orders_daily",
        asset_ref: "FavnView.Assets.customer_orders_daily",
        display_name: "customer_orders_daily",
        secondary: "Stage 1",
        status: "Failed",
        status_tone: :error,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "2.1 s",
        error: "Warehouse timeout",
        inspectable?: true
      }
    ]
  end

  defp sample_asset_results(_status) do
    [
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-raw_orders",
        asset_ref: "FavnView.Assets.raw_orders",
        display_name: "raw_orders",
        secondary: "Stage 0",
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:00 UTC",
        duration: "1.3 s",
        error: nil,
        inspectable?: true
      },
      %{
        id: "run_01jrun_detail_sample-FavnView-Assets-customer_orders_daily",
        asset_ref: "FavnView.Assets.customer_orders_daily",
        display_name: "customer_orders_daily",
        secondary: "Stage 1",
        status: "Succeeded",
        status_tone: :success,
        started_at: "Jun 12, 2026 14:00:02 UTC",
        duration: "1.8 s",
        error: nil,
        inspectable?: true
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
        asset: nil,
        summary: "Run accepted by orchestrator"
      },
      %{
        sequence: 2,
        timestamp: "Jun 12, 2026 14:00:02 UTC",
        event_type: "Step failed",
        status: "Failed",
        status_tone: :error,
        asset: "FavnView.Assets.customer_orders_daily",
        summary: "Asset FavnView.Assets.customer_orders_daily"
      },
      %{
        sequence: 3,
        timestamp: "Jun 12, 2026 14:00:04 UTC",
        event_type: "Run failed",
        status: "Failed",
        status_tone: :error,
        asset: nil,
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
        asset: nil,
        summary: "Run accepted by orchestrator"
      },
      %{
        sequence: 2,
        timestamp: "Jun 12, 2026 14:00:04 UTC",
        event_type: if(status == :running, do: "Step started", else: "Run finished"),
        status: status_label(status),
        status_tone: status_tone(status),
        asset: if(status == :running, do: "FavnView.Assets.customer_orders_daily", else: nil),
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
      %{id: :overview, label: "Overview", icon: "hero-calendar-days"},
      %{id: :events, label: "Events", icon: "hero-signal"},
      %{id: :outputs, label: "Outputs", icon: "hero-circle-stack"},
      %{id: :context, label: "Context", icon: "hero-book-open"},
      %{id: :debug, label: "Debug", icon: "hero-code-bracket"}
    ]
  end

  defp run_facts(%{found?: true} = run) do
    [
      %{label: "Started", value: run.started_at},
      %{label: "Duration", value: run.duration},
      %{label: "Triggered by", value: run.trigger}
    ]
  end

  defp run_facts(_run), do: []

  defp page_title(%{found?: true, title: title}, _run_id), do: title
  defp page_title(_run, run_id), do: "Run #{short_id(run_id)}"

  defp page_subtitle(%{found?: true, subtitle: subtitle}), do: subtitle
  defp page_subtitle(_run), do: "Run detail"

  defp sample_context do
    [
      %{label: "Run ID", value: "run_01jrun_detail_sample"},
      %{label: "Manifest version", value: "mv_customer_orders"},
      %{label: "Trigger", value: "Manual"},
      %{label: "Window", value: "window:day:2026-06-12"}
    ]
  end

  defp sample_current_activity(status) when status in [:pending, :running],
    do: "Currently executing FavnView.Assets.customer_orders_daily"

  defp sample_current_activity(_status), do: nil

  defp sample_failure_summary(status) when status in [:error, :timed_out] do
    %{
      count: 1,
      total: 2,
      asset: "FavnView.Assets.customer_orders_daily",
      error: "Warehouse timeout"
    }
  end

  defp sample_failure_summary(_status), do: nil

  defp sample_no_result_failure_summary(status) when status in [:error, :timed_out] do
    %{count: 0, total: 0, asset: nil, error: "Warehouse timeout"}
  end

  defp sample_no_result_failure_summary(_status), do: nil

  defp sample_asset_empty_message(status) when status in [:pending, :running],
    do: "Run accepted. Waiting for asset execution results..."

  defp sample_asset_empty_message(:ok),
    do: "Run completed, but no asset results were persisted."

  defp sample_asset_empty_message(status) when status in [:error, :timed_out],
    do: "Run failed before asset results were persisted. Latest error: Warehouse timeout"

  defp sample_asset_empty_message(_status), do: "No asset results persisted for this run yet."

  defp status_label(:ok), do: "Succeeded"
  defp status_label(:pending), do: "Pending"
  defp status_label(:running), do: "Running"
  defp status_label(:error), do: "Failed"
  defp status_label(:partial), do: "Partial"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:timed_out), do: "Timed out"
  defp status_label(nil), do: "Unknown"

  defp status_label(status),
    do: status |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp status_tone(:ok), do: :success
  defp status_tone(:pending), do: :info
  defp status_tone(:running), do: :info
  defp status_tone(:error), do: :error
  defp status_tone(:timed_out), do: :error
  defp status_tone(:partial), do: :warning
  defp status_tone(:cancelled), do: :neutral
  defp status_tone(_status), do: :neutral

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
