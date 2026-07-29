defmodule FavnView.Components.ScheduleDetailPage do
  @moduledoc """
  Schedule detail shell and overview components.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.ModeRail
  alias FavnView.Components.Navigation
  alias FavnView.Components.ScheduleUi

  attr :schedule, :map, default: nil
  attr :occurrence_preview, :list, default: []
  attr :occurrence_error, :any, default: nil
  attr :activation_error, :string, default: nil
  attr :active_view, :atom, default: :overview
  attr :loading, :boolean, default: false
  attr :error, :any, default: nil
  attr :nav_items, :list, required: true

  def schedule_detail_page(assigns) do
    assigns = assign_detail_header(assigns)

    ~H"""
    <AppShell.app_shell
      title={@title}
      subtitle={@subtitle}
      status={@status}
      status_tone={@status_tone}
      nav_items={@nav_items}
      back_href={~p"/schedules"}
      back_label="Back to schedules"
      facts={@facts}
      content_scroll?={true}
    >
      <:actions :if={@schedule}>
        <.schedule_actions schedule={@schedule} />
      </:actions>

      <div
        id="schedule-detail-page"
        class="mx-auto w-full max-w-[120rem] overflow-x-hidden pb-24 lg:pb-0"
        data-testid="schedule-detail-page"
      >
        <.loading_state :if={@loading} label="Loading schedule" />
        <.empty_state
          :if={!@loading && @error == :not_found}
          title="Schedule not found"
          description="The schedule is not present in the active manifest."
          icon="hero-calendar-days"
          data-testid="schedule-not-found-state"
        />
        <.error_state
          :if={!@loading && @error && @error != :not_found}
          title="Could not load schedule"
          description={to_string(@error)}
          data-testid="schedule-detail-error-state"
        />
        <div :if={!@loading && !@error && @schedule}>
          <main
            class="min-w-0 overflow-x-hidden space-y-4"
            data-testid={"schedule-detail-#{@active_view}"}
          >
            <.notice
              :if={@activation_error}
              tone={:error}
              icon="hero-exclamation-triangle"
              data-testid="schedule-activation-error"
            >
              {@activation_error}
            </.notice>

            <.status_cards schedule={@schedule} />
            <.panel
              padding={:none}
              class="p-0"
              data-testid={
                if(@active_view == :overview,
                  do: "schedule-overview-panel",
                  else: "schedule-mode-panel"
                )
              }
            >
              <div class="p-3 sm:p-4 lg:p-5">
                <.overview_panel
                  :if={@active_view == :overview}
                  schedule={@schedule}
                  occurrence_preview={@occurrence_preview}
                  occurrence_error={@occurrence_error}
                />
                <.occurrences_panel
                  :if={@active_view == :occurrences}
                  schedule={@schedule}
                  occurrences={@occurrence_preview}
                  error={@occurrence_error}
                />
              </div>
            </.panel>
          </main>
        </div>
      </div>

      <:mode_rail>
        <ModeRail.mode_rail active={@active_view} modes={detail_modes()} on_select="set_detail_view" />
      </:mode_rail>
    </AppShell.app_shell>
    """
  end

  @doc """
  Header controls for one schedule.

  The activation control is the page's primary action, because a schedule an
  operator is looking at is one they are deciding about. Which direction it
  offers, and whether it confirms first, follows from the activation state — see
  `activation_control/1`.
  """
  attr :schedule, :map, required: true

  def schedule_actions(assigns) do
    assigns = assign(assigns, :activation, activation_control(assigns.schedule))

    ~H"""
    <div class="flex flex-wrap items-center gap-2">
      <.button
        variant={@activation.variant}
        icon={@activation.icon}
        phx-click="set_schedule_activation"
        phx-value-action={@activation.action}
        phx-disable-with={@activation.pending_label}
        data-confirm={@activation.confirm}
        data-testid="set-schedule-activation"
      >
        {@activation.label}
      </.button>

      <button
        type="button"
        class="btn btn-sm favn-surface-control rounded-field gap-2"
        data-copy-text={@schedule.id}
        data-testid="copy-detail-schedule-id"
      >
        <.icon name="hero-clipboard-document" class="size-4" /> Copy id
      </button>

      <span class="badge badge-sm favn-surface-control gap-2" data-testid="schedule-manifest-control">
        <.icon name="hero-code-bracket" class="size-4" /> Managed by manifest
      </span>
    </div>
    """
  end

  @doc """
  The activation control one schedule state offers.

  Disabling is destructive — future occurrences stop being submitted — so it is
  a `:danger` button behind a confirmation. `:needs_review` means the definition
  changed after it was approved, so enabling approves the current definition and
  says so rather than pretending nothing changed.

  ## Examples

      iex> FavnView.Components.ScheduleDetailPage.activation_control(
      ...>   %{activation_state: :enabled}
      ...> ).action
      "disable"

      iex> FavnView.Components.ScheduleDetailPage.activation_control(
      ...>   %{activation_state: :disabled}
      ...> ).action
      "enable"

      iex> FavnView.Components.ScheduleDetailPage.activation_control(
      ...>   %{activation_state: :needs_review}
      ...> ).label
      "Approve and enable"
  """
  @spec activation_control(map()) :: %{
          action: String.t(),
          label: String.t(),
          pending_label: String.t(),
          icon: String.t(),
          variant: atom(),
          confirm: String.t() | nil
        }
  def activation_control(%{activation_state: :enabled}) do
    %{
      action: "disable",
      label: "Disable",
      pending_label: "Disabling…",
      icon: "hero-pause-circle",
      variant: :danger,
      confirm: "Stop submitting future runs for this schedule?"
    }
  end

  def activation_control(%{activation_state: :needs_review}) do
    %{
      action: "enable",
      label: "Approve and enable",
      pending_label: "Approving…",
      icon: "hero-power",
      variant: :primary,
      confirm: "The definition changed after it was approved. Enable the current definition?"
    }
  end

  def activation_control(_schedule) do
    %{
      action: "enable",
      label: "Enable",
      pending_label: "Enabling…",
      icon: "hero-power",
      variant: :primary,
      confirm: nil
    }
  end

  attr :schedule, :map, required: true

  def status_cards(assigns) do
    ~H"""
    <section class="space-y-3" data-testid="schedule-overview-cards">
      <span class="sr-only" data-testid="schedule-entry-id">{@schedule.id}</span>
      <div class="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <.status_card
          label="Activation"
          icon="hero-power"
          tone={@schedule.activation_state}
          value={@schedule.activation_label}
          detail={activation_secondary(@schedule)}
        />
        <.status_card
          label="Runtime state"
          icon="hero-arrow-path"
          tone={@schedule.runtime_state}
          value={@schedule.runtime_label}
          detail={runtime_secondary(@schedule)}
        />
        <.status_card
          label="Next due"
          icon="hero-calendar-days"
          tone={:info}
          value={@schedule.next_due_label}
          detail={@schedule.timezone}
        />
        <.status_card
          label="Last submitted"
          icon="hero-paper-airplane"
          tone={:neutral}
          value={@schedule.last_submitted_label}
          detail={@schedule.timezone}
        />
      </div>
    </section>
    """
  end

  attr :label, :string, required: true
  attr :icon, :string, required: true
  attr :tone, :atom, required: true
  attr :value, :string, required: true
  attr :detail, :string, default: nil

  def status_card(assigns) do
    ~H"""
    <div class="favn-surface-list rounded-box p-4">
      <div class="flex items-center gap-3">
        <span class={[
          "flex size-10 shrink-0 items-center justify-center rounded-full",
          icon_tone_class(@tone)
        ]}>
          <.icon name={@icon} class="size-5" />
        </span>

        <div class="min-w-0">
          <p class="text-xs favn-text-muted">{@label}</p>

          <p class="truncate text-2xl font-light tracking-tight text-base-content">{@value}</p>

          <p :if={@detail} class="truncate text-xs favn-text-subtle">{@detail}</p>
        </div>
      </div>
    </div>
    """
  end

  attr :schedule, :map, required: true
  attr :occurrence_preview, :list, required: true
  attr :occurrence_error, :any, default: nil

  def overview_panel(assigns) do
    ~H"""
    <div class="space-y-5" data-testid="schedule-overview-content">
      <.scheduler_error_notice :if={@schedule.last_scheduler_error} schedule={@schedule} />
      <div class="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(22rem,0.85fr)]">
        <.configuration_panel schedule={@schedule} />
        <div class="space-y-5">
          <.preview_summary_panel occurrence_preview={@occurrence_preview} error={@occurrence_error} />
          <.runtime_panel schedule={@schedule} />
        </div>
      </div>
    </div>
    """
  end

  attr :schedule, :map, required: true

  def configuration_panel(assigns) do
    ~H"""
    <section
      class="overflow-hidden rounded-box border border-base-content/10 bg-base-100/10"
      data-testid="schedule-configuration-panel"
    >
      <.panel_header title="Schedule configuration" />
      <dl class="divide-y divide-base-content/10 px-5 py-2 text-sm">
        <.config_row label="Cron" value={@schedule.cron} icon="hero-clock" />
        <.config_row label="Timezone" value={@schedule.timezone} icon="hero-globe-alt" />
        <.config_row
          label="Overlap policy"
          value={policy_label(@schedule.overlap)}
          icon="hero-no-symbol"
        />
        <.config_row label="Missed policy" value={policy_label(@schedule.missed)} icon="hero-forward" />
        <.config_row label="Window" value={@schedule.window_label} icon="hero-calendar" />
        <.config_row
          label="Manifest active"
          value={yes_no(@schedule.manifest_active?)}
          icon="hero-document-check"
        />
        <.config_row
          label="Effective enabled"
          value={yes_no(@schedule.effective_enabled?)}
          icon="hero-bolt"
        />
      </dl>
    </section>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :icon, :string, required: true

  def config_row(assigns) do
    ~H"""
    <div class="grid grid-cols-[1.2rem_9rem_minmax(0,1fr)] items-center gap-3 py-3">
      <.icon name={@icon} class="size-4 text-primary" />
      <dt class="favn-text-muted">{@label}</dt>

      <dd class="min-w-0 truncate font-medium text-base-content">{@value}</dd>
    </div>
    """
  end

  attr :occurrence_preview, :list, required: true
  attr :error, :any, default: nil

  def preview_summary_panel(assigns) do
    ~H"""
    <section
      class="overflow-hidden rounded-box border border-base-content/10 bg-base-100/10"
      data-testid="schedule-occurrence-preview-panel"
    >
      <.panel_header title="Occurrence preview" />
      <div class="p-5 text-sm favn-text-muted">
        <p :if={@error} class="font-medium text-warning">Preview unavailable</p>

        <p :if={@error} class="mt-2">The orchestrator could not compute occurrences right now.</p>

        <div :if={!@error && @occurrence_preview != []}>
          <p class="font-medium text-base-content">Next previewed occurrence</p>

          <p class="mt-2">{hd(@occurrence_preview).due_label}</p>

          <p class="mt-1 text-xs favn-text-subtle">{hd(@occurrence_preview).window_label}</p>
        </div>

        <p :if={!@error && @occurrence_preview == []} class="font-medium text-base-content">
          No upcoming occurrences
        </p>
      </div>
    </section>
    """
  end

  attr :schedule, :map, required: true

  def scheduler_error_notice(assigns) do
    ~H"""
    <div
      class="rounded-box border border-warning/25 bg-warning/10 p-4 text-sm text-warning-content"
      data-testid="schedule-error-notice"
    >
      <div class="flex gap-3">
        <.icon name="hero-exclamation-triangle" class="mt-0.5 size-5 shrink-0 text-warning" />
        <div class="min-w-0">
          <p class="font-medium text-base-content">Scheduler warning</p>

          <p class="mt-1 favn-text-muted">
            {@schedule.last_scheduler_error.phase_label}: {@schedule.last_scheduler_error.message}
          </p>

          <p class="mt-1 text-xs favn-text-subtle">
            {@schedule.last_scheduler_error.occurred_label} · {@schedule.last_scheduler_error.code_label}
          </p>
        </div>
      </div>
    </div>
    """
  end

  attr :schedule, :map, required: true

  def runtime_panel(assigns) do
    ~H"""
    <section
      class="overflow-hidden rounded-box border border-base-content/10 bg-base-100/10"
      data-testid="schedule-runtime-panel"
    >
      <.panel_header title="Runtime state" />
      <dl class="grid gap-x-8 divide-y divide-base-content/10 px-5 py-2 text-sm md:grid-cols-2 md:divide-y-0">
        <.config_row label="Last evaluated" value={@schedule.last_evaluated_label} icon="hero-clock" />
        <.config_row label="Last due" value={@schedule.last_due_label} icon="hero-radio" />
        <.config_row
          label="Last submitted"
          value={@schedule.last_submitted_label}
          icon="hero-paper-airplane"
        />
        <.config_row label="Queued due" value={@schedule.queued_due_label} icon="hero-inbox-stack" />
        <div class="grid grid-cols-[1.2rem_9rem_minmax(0,1fr)] items-center gap-3 py-3">
          <.icon name="hero-play" class="size-4 text-primary" />
          <dt class="favn-text-muted">In-flight run</dt>

          <dd class="min-w-0 truncate font-medium text-base-content">
            <.link
              :if={@schedule.in_flight_run_id}
              navigate={~p"/runs/#{@schedule.in_flight_run_id}"}
              class="font-mono text-primary hover:underline"
            >
              {@schedule.current_run_label}
            </.link>
             <span :if={!@schedule.in_flight_run_id}>-</span>
          </dd>
        </div>
         <.config_row label="Updated" value={@schedule.updated_label} icon="hero-arrow-path" />
      </dl>
    </section>
    """
  end

  attr :schedule, :map, required: true
  attr :occurrences, :list, required: true
  attr :error, :any, default: nil

  def occurrences_panel(assigns) do
    ~H"""
    <div class="overflow-hidden" data-testid="schedule-occurrences-panel">
      <div class="border-b border-base-content/10 px-5 py-4">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 class="text-lg font-semibold text-base-content">Occurrences</h2>

            <p class="mt-1 text-sm favn-text-muted">
              Upcoming occurrences are computed by the orchestrator from this schedule's cron, timezone, and window policy.
            </p>
          </div>

          <div class="flex flex-wrap gap-2 text-xs">
            <span class="badge badge-soft badge-neutral">Next due {@schedule.next_due_label}</span>
            <span class="badge badge-soft badge-neutral">{@schedule.timezone}</span>
            <span class="badge badge-soft badge-neutral">{@schedule.window_label}</span>
          </div>
        </div>

        <p
          :if={!@schedule.effective_enabled?}
          class="mt-3 rounded-field border border-warning/25 bg-warning/10 px-3 py-2 text-sm favn-text-muted"
          data-testid="schedule-occurrences-disabled-note"
        >
          Occurrences are previewed, but this schedule will not submit until it is enabled.
        </p>
      </div>

      <div :if={@error} class="p-6" data-testid="schedule-occurrences-error">
        <div class="rounded-box border border-warning/25 bg-warning/10 p-4">
          <p class="font-medium text-base-content">Occurrence preview unavailable</p>

          <p class="mt-1 text-sm favn-text-muted">
            The orchestrator returned {@error}. Try again after the scheduler state refreshes.
          </p>
        </div>
      </div>

      <div
        :if={!@error && @occurrences == []}
        class="p-8 text-center"
        data-testid="schedule-occurrences-empty"
      >
        <p class="font-medium text-base-content">No upcoming occurrences</p>

        <p class="mt-1 text-sm favn-text-muted">The orchestrator did not return preview rows.</p>
      </div>

      <ScheduleUi.occurrence_preview_table
        :if={!@error && @occurrences != []}
        occurrences={@occurrences}
      />
    </div>
    """
  end

  attr :title, :string, required: true

  def panel_header(assigns) do
    ~H"""
    <header class="border-b border-base-content/10 px-5 py-3">
      <h2 class="font-semibold text-base-content">{@title}</h2>
    </header>
    """
  end

  def nav_items(active \\ :schedules), do: Navigation.items(active)

  def detail_modes do
    [
      %{id: :overview, label: "Overview", icon: "hero-home"},
      %{id: :occurrences, label: "Occurrences", icon: "hero-calendar-days"}
    ]
  end

  defp assign_detail_header(%{schedule: nil} = assigns) do
    assigns
    |> assign(:title, "Schedule")
    |> assign(:subtitle, "Schedule detail")
    |> assign(:status, nil)
    |> assign(:status_tone, :neutral)
    |> assign(:facts, [])
  end

  defp assign_detail_header(%{schedule: schedule} = assigns) do
    assigns
    |> assign(:title, schedule.schedule_label)
    |> assign(:subtitle, schedule.pipeline_label)
    |> assign(:status, schedule.activation_label)
    |> assign(:status_tone, schedule.activation_tone)
    |> assign(:facts, schedule_facts(schedule))
  end

  defp schedule_facts(schedule) do
    [
      %{label: "Cron", value: schedule.cron},
      %{label: "Timezone", value: schedule.timezone},
      %{label: "Window", value: schedule.window_label}
    ]
  end

  defp activation_secondary(%{activation_state: :enabled}), do: "Allowed to submit future runs"

  defp activation_secondary(%{activation_state: :pending_activation}),
    do: "Awaiting operator activation"

  defp activation_secondary(%{activation_state: :needs_review}),
    do: "Schedule fingerprint changed"

  defp activation_secondary(%{activation_state: :disabled}), do: "Operator disabled"
  defp activation_secondary(_schedule), do: "Not enabled"

  defp runtime_secondary(%{in_flight_run_id: run_id}) when is_binary(run_id), do: run_id
  defp runtime_secondary(%{runtime_state: :queued}), do: "Queued occurrence exists"
  defp runtime_secondary(%{runtime_state: :idle}), do: "Ready"
  defp runtime_secondary(_schedule), do: "Not submitting"

  defp icon_tone_class(:enabled), do: "border-success/25 bg-success/10 text-success"
  defp icon_tone_class(:running), do: "border-info/25 bg-info/10 text-info"
  defp icon_tone_class(:queued), do: "border-warning/25 bg-warning/10 text-warning"
  defp icon_tone_class(:pending_activation), do: "border-warning/25 bg-warning/10 text-warning"
  defp icon_tone_class(:needs_review), do: "border-warning/25 bg-warning/10 text-warning"
  defp icon_tone_class(:disabled), do: "border-error/25 bg-error/10 text-error"
  defp icon_tone_class(:info), do: "border-info/25 bg-info/10 text-info"
  defp icon_tone_class(_tone), do: "border-base-content/15 bg-base-200/40 favn-text-muted"

  defp policy_label(nil), do: "-"

  defp policy_label(value),
    do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp yes_no(true), do: "Yes"
  defp yes_no(false), do: "No"
  defp yes_no(_value), do: "-"
end
