defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Backfill-centric run detail page for operator inspection.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail
  alias FavnView.Components.RunDetailPage.AttemptDrawer
  alias FavnView.Components.RunDetailPage.Events
  alias FavnView.Components.RunDetailPage.Failures
  alias FavnView.Components.RunDetailPage.NotFound
  alias FavnView.Components.RunDetailPage.Overview
  alias FavnView.Components.RunDetailPage.Samples
  alias FavnView.Components.RunDetailPage.Stats
  alias FavnView.Components.RunDetailPage.Timeline
  alias FavnView.Components.RunDetailPage.WindowRuns

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :active_mode, :atom, default: :overview
  attr :timeline_state, :map, default: nil
  attr :timeline_hook?, :boolean, default: false
  attr :selected_child_run_id, :string, default: nil
  attr :selected_attempt_id, :string, default: nil

  def run_detail_page(assigns) do
    run = normalize_run(assigns.run)

    assigns =
      assigns
      |> assign(:run, run)
      |> assign(:selected_attempt, selected_attempt(run, assigns.selected_attempt_id))

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
      <:compact_header_action :if={@run[:cancellable?]}>
        <button
          type="button"
          class="btn btn-error btn-soft btn-sm gap-2 rounded-box border-error/30"
          phx-click="cancel_run"
          phx-disable-with="Cancelling..."
          data-confirm="Cancel this run? Active runner work will be asked to stop."
          data-testid="cancel-run-button"
        >
          <.icon name="hero-no-symbol" class="size-4" /> {@run[:cancel_label] || "Cancel run"}
        </button>
      </:compact_header_action>
      <:compact_header_action :if={@run[:retry_remaining?]}>
        <button
          type="button"
          class="btn btn-primary btn-soft btn-sm gap-2 rounded-box border-primary/30"
          phx-click="retry_remaining"
          phx-disable-with="Submitting..."
          data-confirm="Retry remaining failed or not-started assets with the same run configuration?"
          data-testid="retry-remaining-button"
        >
          <.icon name="hero-arrow-path" class="size-4" /> {@run[:retry_remaining_label] ||
            "Retry remaining"}
        </button>
      </:compact_header_action>

      <NotFound.not_found_panel :if={!@run[:found?]} run={@run} />
      <.execution_group_page
        :if={@run[:found?]}
        run={@run}
        active_mode={@active_mode}
        timeline_state={@timeline_state || default_timeline_state(@run)}
        timeline_hook?={@timeline_hook?}
        selected_child_run_id={@selected_child_run_id}
        selected_attempt={@selected_attempt}
      />

      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={run_modes(@run)} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :run, :map, required: true
  attr :active_mode, :atom, required: true
  attr :timeline_state, :map, required: true
  attr :timeline_hook?, :boolean, required: true
  attr :selected_child_run_id, :string, default: nil
  attr :selected_attempt, :map, default: nil

  def execution_group_page(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-[120rem] flex-col gap-3" data-testid="run-detail-page">
      <Stats.execution_group_stats run={@run} />

      <GlassPanel.glass_panel
        class="p-0"
        data-testid={if(@active_mode == :overview, do: "run-overview-panel", else: "run-mode-panel")}
        data-run-active={to_string(@run.active?)}
      >
        <div class="p-3 sm:p-4 lg:p-5">
          <Overview.overview_panel :if={@active_mode == :overview} run={@run} />
          <Timeline.timeline_panel
            :if={@active_mode == :timeline}
            run={@run}
            timeline_state={@timeline_state}
            timeline_hook?={@timeline_hook?}
          />
          <Failures.failures_panel :if={@active_mode == :failures} run={@run} />
          <WindowRuns.window_runs_panel
            :if={@active_mode == :windows}
            run={@run}
            selected_child_run_id={@selected_child_run_id}
          />
          <Events.events_panel :if={@active_mode == :events} run={@run} />
        </div>
      </GlassPanel.glass_panel>

      <AttemptDrawer.attempt_drawer :if={@selected_attempt} attempt={@selected_attempt} />
    </div>
    """
  end

  defdelegate sample_run(status \\ :running), to: Samples
  defdelegate empty_run(), to: Samples
  defdelegate sample_run_with_no_results(status), to: Samples
  defdelegate sample_run_with_long_asset_names(), to: Samples
  defdelegate sample_run_with_node_statuses(), to: Samples
  defdelegate sample_nav_items(), to: Samples
  defdelegate sample_full_refresh_run(), to: Samples
  defdelegate sample_single_window_run(), to: Samples
  defdelegate sample_timeline_run(), to: Samples
  defdelegate sample_completed_timeline_run(), to: Samples
  defdelegate sample_admission_failed_backfill(), to: Samples
  defdelegate not_found_run(), to: Samples
  defdelegate unavailable_run(), to: Samples
  defdelegate sample_execution_group(status \\ :running), to: Samples

  defp selected_attempt(%{attempts: attempts}, attempt_id) when is_binary(attempt_id),
    do: Enum.find(attempts, &(&1.id == attempt_id))

  defp selected_attempt(_run, _attempt_id), do: nil

  defp normalize_run(run) when is_map(run) do
    run
    |> Map.put_new(:failures, [])
    |> Map.put_new(:backfill_failures, [])
    |> Map.put_new(:backfill_failure_count, 0)
    |> Map.put_new(:retry_remaining?, false)
  end

  defp default_timeline_state(%{active?: true}) do
    live_timeline_state()
  end

  defp default_timeline_state(%{running_asset_attempts: running}) when running > 0 do
    live_timeline_state()
  end

  defp default_timeline_state(%{raw_status: status}) when status in [:pending, :running] do
    live_timeline_state()
  end

  defp default_timeline_state(_run) do
    %{
      mode: :fit,
      zoom: "full",
      live_follow?: false,
      search: "",
      status: "all",
      window: "all",
      failed_only?: false,
      running_only?: false
    }
  end

  defp live_timeline_state do
    %{
      mode: :live,
      zoom: "30m",
      live_follow?: true,
      search: "",
      status: "all",
      window: "all",
      failed_only?: false,
      running_only?: false
    }
  end

  defp run_modes(run) do
    [
      %{id: :overview, label: "Overview", icon: "hero-table-cells"},
      %{id: :timeline, label: "Timeline", icon: "hero-chart-bar"},
      %{
        id: :failures,
        label: "Failures",
        icon: "hero-exclamation-triangle",
        count: failed_count(run)
      },
      %{
        id: :windows,
        label: "Window runs",
        icon: "hero-rectangle-stack",
        count: run[:total_windows]
      },
      %{id: :events, label: "Events", icon: "hero-signal"}
    ]
  end

  defp run_facts(%{found?: true} = run) do
    [
      %{label: "Started", value: run.started_at},
      %{label: "Duration", value: run.elapsed_duration},
      %{label: "Trigger", value: run.trigger}
    ]
  end

  defp run_facts(_run), do: []
  defp page_title(%{found?: true, title: title}, _run_id), do: title
  defp page_title(_run, run_id), do: "Run #{short_id(run_id)}"
  defp page_subtitle(%{found?: true, subtitle: subtitle}), do: subtitle
  defp page_subtitle(_run), do: "Run detail"

  defp failed_count(run),
    do: (run[:failed_asset_attempts] || 0) + (run[:backfill_failure_count] || 0)

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
