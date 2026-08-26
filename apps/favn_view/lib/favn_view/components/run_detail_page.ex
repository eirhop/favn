defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Run detail for one exact run, with lean assets and lazy events.

  Windowed runs are separate run IDs. A run that belongs to a backfill shows a
  calendar rail of its sibling window runs, loaded with the run rather than
  behind a button; selecting a cell patches the page to that run in place.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.ModeRail
  alias FavnView.Components.RunDetailPage.Events
  alias FavnView.Components.RunDetailPage.Flow
  alias FavnView.Components.RunDetailPage.NotFound
  alias FavnView.Components.RunDetailPage.Progress
  alias FavnView.Components.RunDetailPage.Submission
  alias FavnView.Components.RunDetailPage.WindowFailures
  alias FavnView.Components.RunDetailPage.WindowRail
  alias FavnView.RunWindowRail

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :active_mode, :atom, default: :flow
  attr :flow_view, :atom, default: :chart
  attr :flow_filter, :list, default: []
  attr :flow_sort, :atom, default: :start

  attr :rail, :any, default: nil
  attr :compare?, :boolean, default: false
  attr :compare_limit_reached?, :boolean, default: false
  attr :compare_error, :string, default: nil
  attr :windows_error, :string, default: nil
  attr :window_failures, :list, default: nil
  attr :window_failures_overflow?, :boolean, default: false
  attr :window_failures_error, :string, default: nil
  attr :flash, :map, default: %{}

  def run_detail_page(assigns) do
    assigns = assign(assigns, :run, normalize_run(assigns.run))

    ~H"""
    <AppShell.app_shell
      title={page_title(@run, @run_id)}
      subtitle={page_subtitle(@run)}
      status={@run[:status]}
      status_tone={@run[:status_tone] || :neutral}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      back_href={@run[:back_asset_href]}
      back_label={if(@run[:back_asset_href], do: "Back to asset", else: nil)}
      facts={run_facts(@run)}
      flash={@flash}
    >
      <:actions :if={@run[:cancellable?]}>
        <.button
          variant={:danger}
          icon="hero-no-symbol"
          phx-click="cancel_run"
          data-command-operation="run_cancel"
          data-command-resource={@run[:cancel_run_id] || @run_id}
          phx-disable-with="Cancelling..."
          data-confirm="Cancel this run? Active runner work will be asked to stop."
          data-testid="cancel-run-button"
        >
          {@run[:cancel_label] || "Cancel run"}
        </.button>
      </:actions>

      <:actions :if={@run[:retry_remaining?]}>
        <.button
          icon="hero-arrow-path"
          phx-click="retry_remaining"
          data-command-operation="run_retry_remaining"
          data-command-resource={@run_id}
          phx-disable-with="Submitting..."
          data-confirm="Retry remaining failed or not-started assets with the same run configuration?"
          data-testid="retry-remaining-button"
        >
          {@run[:retry_remaining_label] || "Retry remaining"}
        </.button>
      </:actions>
      <Submission.submission_panel :if={@run[:submission?]} run={@run} />
      <NotFound.not_found_panel :if={!@run[:found?] && !@run[:submission?]} run={@run} />
      <.execution_group_page
        :if={@run[:found?]}
        run={@run}
        active_mode={@active_mode}
        flow_view={@flow_view}
        flow_filter={@flow_filter}
        flow_sort={@flow_sort}
        rail={@rail}
        compare?={@compare?}
        compare_limit_reached?={@compare_limit_reached?}
        compare_error={@compare_error}
        windows_error={@windows_error}
        window_failures={@window_failures}
        window_failures_overflow?={@window_failures_overflow?}
        window_failures_error={@window_failures_error}
      />
      <:mode_rail :if={@run[:found?]}>
        <ModeRail.mode_rail active={@active_mode} modes={run_modes(@run)} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :run, :map, required: true
  attr :active_mode, :atom, required: true
  attr :flow_view, :atom, default: :chart
  attr :flow_filter, :list, default: []
  attr :flow_sort, :atom, default: :start
  attr :rail, :any, default: nil
  attr :compare?, :boolean, default: false
  attr :compare_limit_reached?, :boolean, default: false
  attr :compare_error, :string, default: nil
  attr :windows_error, :string, default: nil
  attr :window_failures, :list, default: nil
  attr :window_failures_overflow?, :boolean, default: false
  attr :window_failures_error, :string, default: nil

  def execution_group_page(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-[110rem] flex-col gap-4" data-testid="run-detail-page">
      <Progress.run_progress run={@run} />
      <WindowRail.window_rail
        :if={@rail}
        rail={@rail}
        compare?={@compare?}
        limit_reached?={@compare_limit_reached?}
      />
      <%!-- Telling the operator to open a window run when the backfill produced
      none is the page lying about what it offers. A backfill whose windows all
      failed before a run existed has nothing to open, and that is the single
      most useful thing this screen can say. --%>
      <.notice
        :if={@run[:backfill_parent?] && windows_to_open?(@run, @rail)}
        tone={:info}
        icon="hero-calendar-days"
        data-testid="backfill-parent-explanation"
      >
        This is the backfill parent run. Asset work is executed by its window runs. Open a
        window run to inspect its assets and results.
      </.notice>

      <.notice
        :if={@run[:backfill_parent?] && !windows_to_open?(@run, @rail)}
        tone={no_run_tone(@run)}
        icon="hero-exclamation-triangle"
        data-testid="backfill-parent-no-window-runs"
      >
        {no_window_runs_message(@run)}
      </.notice>

      <.notice :if={@run[:group_error]} tone={:warning} icon="hero-arrow-path">
        {@run.group_error}
      </.notice>

      <.notice
        :if={@run.asset_attempts_truncated?}
        tone={:warning}
        icon="hero-scissors"
        data-testid="run-detail-truncated-warning"
      >
        This run has more than 1,000 assets. Summary counts remain exact; this page shows the
        first 1,000 in stable order.
      </.notice>

      <.notice :if={@run[:refresh_error]} tone={:warning} icon="hero-arrow-path">
        {@run.refresh_error}. Showing the last successful result; the page will try again.
      </.notice>

      <.notice
        :if={@windows_error}
        tone={:warning}
        icon="hero-exclamation-triangle"
        data-testid="window-read-warning"
      >
        {@windows_error}
      </.notice>

      <.notice
        :if={@compare_error}
        tone={:warning}
        icon="hero-square-2-stack"
        data-testid="compare-warning"
      >
        {@compare_error}
      </.notice>

      <%!-- The reasons sit above Flow because on a backfill whose windows never
      ran they are the only account of what happened; Flow below has nothing to
      show. On a partly failed backfill they read as the exception list under the
      rail that offers the windows that did run. --%>
      <WindowFailures.window_failures
        :if={@run[:backfill_parent?]}
        groups={@window_failures}
        truncated?={@window_failures_overflow?}
        error={@window_failures_error}
        failed_windows={@run[:failed_windows] || 0}
      />

      <div data-run-active={to_string(@run.active?)}>
        <Flow.flow
          :if={@active_mode == :flow}
          assets={@run.assets}
          chart={@run[:chart]}
          comparison={@run[:comparison]}
          view={@flow_view}
          filter={@flow_filter}
          sort={@flow_sort}
          backfill_parent?={@run[:backfill_parent?] || false}
          windows_to_open?={windows_to_open?(@run, @rail)}
        />
        <Events.events_panel :if={@active_mode == :events} run={@run} />
      </div>
    </div>
    """
  end

  defp normalize_run(run) when is_map(run) do
    run
    |> Map.put_new(:assets, [])
    |> Map.put_new(:retry_remaining?, false)
    |> Map.put_new(:asset_attempts_truncated?, false)
  end

  defp run_modes(_run) do
    [
      %{id: :flow, label: "Flow", icon: "hero-chart-bar"},
      %{id: :events, label: "Events", icon: "hero-signal"}
    ]
  end

  defp run_facts(%{found?: true} = run) do
    # A combined backfill has no window rail to state its coverage, because it
    # runs every window it covers as one run. The span is a property of the run,
    # so it belongs beside the run's other properties.
    [
      %{label: "Started", value: run.started_at},
      %{label: "Duration", value: run.elapsed_duration},
      %{label: "Trigger", value: run.trigger}
    ] ++ combined_window_fact(run)
  end

  defp run_facts(%{submission?: true} = run) do
    [
      %{label: "Queued", value: run.enqueued_at || "-"},
      %{label: "Target", value: run.target_id, mono: true},
      %{label: "Attempt", value: run.attempt}
    ]
  end

  defp run_facts(_run), do: []

  defp combined_window_fact(%{combined_window: %{label: label, window_count: count}}),
    do: [%{label: "Combined window", value: "#{label} · #{count} windows"}]

  defp combined_window_fact(_run), do: []

  # The rail is the only thing that can offer a window run, so it decides
  # whether there is anything to open. A backfill still creating windows has
  # something coming and is not reported as having produced nothing.
  defp windows_to_open?(run, rail) do
    cond do
      match?(%RunWindowRail{cells: [_ | _]}, rail) -> true
      match?(%RunWindowRail{combined: %{}}, rail) -> true
      match?(%RunWindowRail{in_progress?: true}, rail) -> true
      (run[:total_windows] || 0) == 0 -> true
      run[:active?] -> true
      true -> false
    end
  end

  defp no_run_tone(run) do
    if (run[:failed_windows] || 0) > 0, do: :error, else: :warning
  end

  # Says how many windows the backfill planned, because that count is the only
  # description of the covered range this page holds once no window run exists
  # to name its own window. It does not send the operator to the per-window
  # failure reason: that reason is stored, but no operator surface reads it yet,
  # and pointing at a place the product does not have is worse than silence.
  defp no_window_runs_message(run) do
    total = run[:total_windows] || 0
    failed = run[:failed_windows] || 0

    cond do
      failed >= total and total > 0 ->
        "None of the #{total} #{plural(total, "window")} produced a run: every one failed " <>
          "before a run started, so there is nothing to open."

      total > 0 ->
        "This backfill planned #{total} #{plural(total, "window")} and none of them has a run " <>
          "yet, so there is nothing to open."

      true ->
        "This backfill produced no window runs."
    end
  end

  defp plural(1, word), do: word
  defp plural(_count, word), do: word <> "s"
  defp page_title(%{found?: true, title: title}, _run_id), do: title
  defp page_title(_run, run_id), do: "Run #{short_id(run_id)}"
  defp page_subtitle(%{found?: true, subtitle: subtitle}), do: subtitle

  defp page_subtitle(%{submission?: true, target_kind: kind}),
    do: "#{String.capitalize(kind)} request"

  defp page_subtitle(_run), do: "Run detail"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
