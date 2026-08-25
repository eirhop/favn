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
  alias FavnView.Components.RunDetailPage.WindowRail

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :active_mode, :atom, default: :flow

  attr :rail, :any, default: nil
  attr :windows_error, :string, default: nil
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
        rail={@rail}
        windows_error={@windows_error}
      />
      <:mode_rail :if={@run[:found?]}>
        <ModeRail.mode_rail active={@active_mode} modes={run_modes(@run)} on_select="set_mode" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  attr :run, :map, required: true
  attr :active_mode, :atom, required: true
  attr :rail, :any, default: nil
  attr :windows_error, :string, default: nil

  def execution_group_page(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-[110rem] flex-col gap-4" data-testid="run-detail-page">
      <Progress.run_progress run={@run} />
      <WindowRail.window_rail :if={@rail} rail={@rail} />
      <.notice
        :if={@run[:backfill_parent?]}
        tone={:info}
        icon="hero-calendar-days"
        data-testid="backfill-parent-explanation"
      >
        This is the backfill parent run. Asset work is executed by its window runs. Open a
        window run to inspect its assets and results.
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

      <.notice :if={@windows_error} tone={:warning} icon="hero-exclamation-triangle">
        {@windows_error}
      </.notice>

      <div data-run-active={to_string(@run.active?)}>
        <Flow.flow
          :if={@active_mode == :flow}
          assets={@run.assets}
          backfill_parent?={@run[:backfill_parent?] || false}
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
    [
      %{label: "Started", value: run.started_at},
      %{label: "Duration", value: run.elapsed_duration},
      %{label: "Trigger", value: run.trigger}
    ]
  end

  defp run_facts(%{submission?: true} = run) do
    [
      %{label: "Queued", value: run.enqueued_at || "-"},
      %{label: "Target", value: run.target_id, mono: true},
      %{label: "Attempt", value: run.attempt}
    ]
  end

  defp run_facts(_run), do: []
  defp page_title(%{found?: true, title: title}, _run_id), do: title
  defp page_title(_run, run_id), do: "Run #{short_id(run_id)}"
  defp page_subtitle(%{found?: true, subtitle: subtitle}), do: subtitle

  defp page_subtitle(%{submission?: true, target_kind: kind}),
    do: "#{String.capitalize(kind)} request"

  defp page_subtitle(_run), do: "Run detail"

  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id), do: to_string(id)
end
