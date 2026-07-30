defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Run detail: one flow of work, and a panel for whatever the operator selects.

  This page used to offer five peer modes — overview, timeline, failures, window
  runs, events — which were five projections of one list of asset attempts. The
  flow replaces the first three: it keeps time like the timeline did, keeps the
  dependency order the timeline threw away, and renders each failure in the lane
  it happened in, so there is nothing left for a failures tab to show.

  Window runs stays because a child run is a different object from an asset
  attempt. Events stays because a raw stream is sometimes the only way to see what
  a stuck run last did.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.ModeRail
  alias FavnView.Components.RunDetailPage.AttemptDrawer
  alias FavnView.Components.RunDetailPage.Events
  alias FavnView.Components.RunDetailPage.Failures
  alias FavnView.Components.RunDetailPage.Flow
  alias FavnView.Components.RunDetailPage.NotFound
  alias FavnView.Components.RunDetailPage.Progress
  alias FavnView.Components.RunDetailPage.WindowRuns
  alias FavnView.RunFlow

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :active_mode, :atom, default: :flow

  attr :flow, :map,
    default: nil,
    doc: "a prebuilt `FavnView.RunFlow` projection; derived from the run when absent"

  attr :selected_child_run_id, :string, default: nil
  attr :selected_attempt_id, :string, default: nil
  attr :flash, :map, default: %{}

  def run_detail_page(assigns) do
    run = normalize_run(assigns.run)

    assigns =
      assigns
      |> assign(:run, run)
      |> assign(:flow, assigns.flow || flow(run))
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
      <NotFound.not_found_panel :if={!@run[:found?]} run={@run} />
      <.execution_group_page
        :if={@run[:found?]}
        run={@run}
        flow={@flow}
        active_mode={@active_mode}
        selected_child_run_id={@selected_child_run_id}
        selected_attempt={@selected_attempt}
        selected_attempt_id={@selected_attempt_id}
      />
      <:mode_rail>
        <ModeRail.mode_rail active={@active_mode} modes={run_modes(@run)} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :run, :map, required: true
  attr :flow, :map, default: nil
  attr :active_mode, :atom, required: true
  attr :selected_child_run_id, :string, default: nil
  attr :selected_attempt, :map, default: nil
  attr :selected_attempt_id, :string, default: nil

  def execution_group_page(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-[110rem] flex-col gap-4" data-testid="run-detail-page">
      <Progress.run_progress run={@run} />
      <.notice
        :if={truncated?(@run)}
        tone={:warning}
        icon="hero-scissors"
        data-testid="run-detail-truncated-warning"
      >
        Showing the first bounded detail slice. The meters above are exact; some detail rows
        are omitted.
      </.notice>
      <Failures.window_failures run={@run} />
      <div data-run-active={to_string(@run.active?)}>
        <Flow.flow
          :if={@active_mode == :flow and @flow}
          flow={@flow}
          selected_attempt_id={@selected_attempt_id}
        />
        <WindowRuns.window_runs_panel
          :if={@active_mode == :windows}
          run={@run}
          selected_child_run_id={@selected_child_run_id}
        /> <Events.events_panel :if={@active_mode == :events} run={@run} />
      </div>
      <AttemptDrawer.attempt_drawer :if={@selected_attempt} attempt={@selected_attempt} />
    </div>
    """
  end

  # The LiveView precomputes the flow so a refresh does not rebuild it inside
  # `render/1`. Every other caller — design-system examples, component tests —
  # only has a run, so derive it here rather than make each of them do it.
  defp flow(%{found?: true, attempts: attempts, active?: active?}),
    do: RunFlow.build(attempts, active?: active?)

  defp flow(_run), do: nil

  defp selected_attempt(%{attempts: attempts}, attempt_id) when is_binary(attempt_id),
    do: Enum.find(attempts, &(&1.id == attempt_id))

  defp selected_attempt(_run, _attempt_id), do: nil

  defp truncated?(run) do
    run.asset_attempts_truncated? or run.requested_windows_truncated? or run.child_runs_truncated?
  end

  defp normalize_run(run) when is_map(run) do
    run
    |> Map.put_new(:failures, [])
    |> Map.put_new(:backfill_failures, [])
    |> Map.put_new(:backfill_failure_count, 0)
    |> Map.put_new(:retry_remaining?, false)
    |> Map.put_new(:requested_windows_truncated?, false)
    |> Map.put_new(:asset_attempts_truncated?, false)
    |> Map.put_new(:child_runs_truncated?, false)
  end

  defp run_modes(run) do
    List.flatten([
      %{id: :flow, label: "Flow", icon: "hero-chart-bar"},
      window_mode(run),
      %{id: :events, label: "Events", icon: "hero-signal"}
    ])
  end

  # A run that executes its assets directly has no child runs, so the mode has
  # nothing to show. A single-window run has exactly one, which is this page —
  # offering it invites a click that lands back where it started.
  defp window_mode(run) do
    case window_run_count(run) do
      nil ->
        []

      count ->
        [
          %{
            id: :windows,
            label: "Window runs",
            icon: "hero-rectangle-stack",
            count: count
          }
        ]
    end
  end

  defp window_run_count(run) do
    total = Map.get(run, :total_windows)
    children = length(Map.get(run, :child_runs, []))

    cond do
      is_integer(total) and total > 1 -> total
      children > 1 -> children
      true -> nil
    end
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

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
