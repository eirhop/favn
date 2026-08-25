defmodule FavnView.Components.RunDetailPage do
  @moduledoc """
  Run detail for one exact run, with lean assets and lazy events.

  Windowed runs are separate run IDs. The page loads their lean choices only
  when the operator asks to switch, then navigates to the selected run.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.ModeRail
  alias FavnView.Components.RunDetailPage.Events
  alias FavnView.Components.RunDetailPage.Flow
  alias FavnView.Components.RunDetailPage.NotFound
  alias FavnView.Components.RunDetailPage.Progress
  alias FavnView.Components.RunDetailPage.Submission

  attr :run, :map, required: true
  attr :run_id, :string, required: true
  attr :nav_items, :list, default: []
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :active_mode, :atom, default: :flow

  attr :windows, :any, default: nil
  attr :windows_loading?, :boolean, default: false
  attr :windows_error, :string, default: nil
  attr :flash, :map, default: %{}

  def run_detail_page(assigns) do
    run = normalize_run(assigns.run)
    navigation_windows = navigation_windows(run, assigns.windows, assigns.run_id)

    assigns = assigns |> assign(:run, run) |> assign(:navigation_windows, navigation_windows)

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
      <:actions :if={@run[:found?] && (@run[:window] || @run[:backfill_parent?]) && is_nil(@windows)}>
        <.button
          variant={:secondary}
          icon="hero-calendar-days"
          phx-click="load_windows"
          loading={@windows_loading?}
          data-testid="load-run-windows"
        >
          {if(@run[:backfill_parent?], do: "Open window run", else: "Switch window")}
        </.button>
      </:actions>
      <:actions :if={
        @run[:found?] && is_list(@navigation_windows) && length(@navigation_windows) == 1
      }>
        <.button
          variant={:secondary}
          icon="hero-arrow-top-right-on-square"
          navigate={~p"/runs/#{hd(@navigation_windows).run_id}"}
          data-testid="open-run-window"
        >
          Open window run
        </.button>
      </:actions>
      <:actions :if={@run[:found?] && is_list(@navigation_windows) && length(@navigation_windows) > 1}>
        <form phx-change="switch_window" data-testid="run-window-selector">
          <.select_field
            name="run_id"
            label="Run window"
            icon="hero-calendar-days"
            value=""
            options={window_options(@navigation_windows)}
            class="min-w-64"
          />
        </form>
      </:actions>
      <Submission.submission_panel :if={@run[:submission?]} run={@run} />
      <NotFound.not_found_panel :if={!@run[:found?] && !@run[:submission?]} run={@run} />
      <.execution_group_page
        :if={@run[:found?]}
        run={@run}
        active_mode={@active_mode}
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
  attr :windows_error, :string, default: nil

  def execution_group_page(assigns) do
    ~H"""
    <div class="mx-auto flex w-full max-w-[110rem] flex-col gap-4" data-testid="run-detail-page">
      <Progress.run_progress run={@run} />
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

  defp window_options(windows) do
    [{"Select a window run", ""} | Enum.map(windows, &{&1.label, &1.run_id})]
  end

  defp navigation_windows(_run, nil, _run_id), do: nil

  defp navigation_windows(%{backfill_parent?: true}, windows, _run_id) when is_list(windows),
    do: windows

  defp navigation_windows(_run, windows, run_id) when is_list(windows),
    do: Enum.reject(windows, &(&1.run_id == run_id))

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
