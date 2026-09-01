defmodule FavnView.Components.RunnersPage do
  @moduledoc """
  Runner health page: live presence, capacity per pool and release, and the
  durable session history that survives runner and control-plane restarts.

  Runner presence, capacity, and session metadata are platform-global; task
  detail inside a session expander stays scoped to the operator's workspace.
  Run and asset outcomes live on the runs pages, not here.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation

  attr :overview, :map, default: nil
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :window, :atom, default: :week
  attr :state, :atom, default: :all
  attr :expanded, :map, default: %{}
  attr :nav_items, :list, required: true
  attr :current_scope, :any, default: nil
  attr :operator_workspaces, :list, default: []
  attr :flash, :map, default: %{}

  def runners_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Runners"
      subtitle={subtitle(@overview)}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      flash={@flash}
    >
      <div
        class="mx-auto flex w-full max-w-[100rem] flex-col gap-4 pb-24 lg:pb-0"
        data-testid="runners-page"
      >
        <div class="flex justify-end">
          <.button
            phx-click="reload"
            icon="hero-arrow-path"
            data-testid="runners-refresh"
          >
            Refresh diagnostics
          </.button>
        </div>
        <.loading_state :if={@loading} label="Loading runner diagnostics" />
        <.error_state
          :if={!@loading && @error}
          title="Could not load runner diagnostics"
          description={@error}
          data-testid="runners-error-state"
        >
          <:action><.button phx-click="reload" icon="hero-arrow-path">Retry</.button></:action>
        </.error_state>
        <.overview
          :if={!@loading && !@error && @overview}
          overview={@overview}
          window={@window}
          state={@state}
          expanded={@expanded}
          current_scope={@current_scope}
        />
      </div>
    </AppShell.app_shell>
    """
  end

  attr :overview, :map, required: true
  attr :window, :atom, required: true
  attr :state, :atom, required: true
  attr :expanded, :map, required: true
  attr :current_scope, :any, required: true

  defp overview(assigns) do
    assigns = assign(assigns, :starved, starved_partitions(assigns.overview))

    ~H"""
    <div class="grid gap-4 lg:grid-cols-3" data-testid="runners-stat-header">
      <.panel data-testid="runner-stats">
        <:header title="Runners" subtitle="Live process presence" icon="hero-server-stack" />
        <div class="flex gap-8">
          <.metric label="Connected" value={@overview.runner_count} />
          <.metric label="Busy" value={@overview.busy_runner_count} />
        </div>
      </.panel>

      <.panel class="lg:col-span-2" data-testid="workspace-task-stats">
        <:header
          title="Workspace tasks"
          subtitle="Durable task counters for this workspace"
          icon="hero-queue-list"
        />
        <div class="flex flex-wrap gap-8">
          <.metric
            label="Queued"
            value={@overview.workspace_tasks.queued_count}
            tone={(@overview.workspace_tasks.queued_count > 0 && :warning) || :neutral}
          />
          <.metric label="Running" value={@overview.workspace_tasks.active_count} />
          <.metric
            label={"Failed · #{failed_window_label(@window)}"}
            value={@overview.workspace_tasks.failed_count}
            tone={(@overview.workspace_tasks.failed_count > 0 && :error) || :neutral}
          />
          <.metric
            label="Longest wait"
            value={wait_label(@overview.workspace_tasks.oldest_queued_at, @overview.observed_at)}
            tone={(@overview.workspace_tasks.oldest_queued_at && :warning) || :neutral}
          />
        </div>
      </.panel>
    </div>

    <.notice
      :if={@starved != []}
      tone={:warning}
      icon="hero-exclamation-triangle"
      data-testid="runners-starved-notice"
    >
      <p class="font-medium">Work is waiting with no compatible runner connected.</p>

      <p class="mt-1">
        {Enum.map_join(@starved, "; ", &starved_label/1)}. Start a runner for the pool and
        release listed, and the queued work resumes on its own.
      </p>
    </.notice>

    <.capacity_panel :if={@overview.capacity != []} overview={@overview} />

    <.panel data-testid="connected-runners">
      <:header
        title="Connected runners"
        subtitle="Live process presence on this control-plane node"
        icon="hero-server-stack"
      />
      <.empty_state
        :if={@overview.registry_status == :available && @overview.runners == []}
        title="No runners connected"
        description="Queued work remains durable and will wait for a compatible runner."
        icon="hero-server-stack"
        data-testid="runners-empty-state"
      />
      <.notice
        :if={@overview.registry_status == :unavailable}
        tone={:error}
        icon="hero-exclamation-triangle"
        data-testid="runner-registry-unavailable"
      >
        <p class="font-medium">Runner registry unavailable</p>

        <p class="mt-1">
          Live runner presence cannot be read on this control-plane node. Durable session
          history remains available below.
        </p>
      </.notice>

      <.stack :if={@overview.runners != []} gap={:sm}>
        <.runner_card
          :for={runner <- @overview.runners}
          runner={runner}
          current_scope={@current_scope}
        />
      </.stack>
    </.panel>

    <.panel data-testid="runner-sessions">
      <:header
        title="Runner sessions"
        subtitle="When each runner woke, what it did, and how it ended"
        icon="hero-clock"
      />
      <div class="flex flex-wrap items-center gap-2">
        <.scope_rail
          label="Session state"
          choices={state_choices(@overview, @state)}
          on_select="set_state"
        />
        <.scope_rail
          label="Session window"
          choices={window_choices(@window)}
          on_select="set_window"
          class="sm:ml-auto"
        />
      </div>

      <p class="mt-3 text-sm favn-text-subtle" data-testid="runner-session-totals">
        {totals_label(@overview.totals, @window)}
      </p>

      <.empty_state
        :if={displayed_sessions(@overview, @state) == []}
        title="No runner sessions in this window"
        description="Runner registrations, shutdowns, and crashes will appear here."
        icon="hero-clock"
        data-testid="runner-sessions-empty-state"
      />

      <.stack :if={displayed_sessions(@overview, @state) != []} gap={:sm} class="mt-3">
        <.session_entry
          :for={entry <- displayed_sessions(@overview, @state)}
          entry={entry}
          expanded={@expanded}
          current_scope={@current_scope}
        />
      </.stack>
    </.panel>
    """
  end

  attr :overview, :map, required: true

  defp capacity_panel(assigns) do
    ~H"""
    <.panel padding={:none} data-testid="runner-capacity">
      <:header
        title="Capacity by pool and release"
        subtitle="Durable demand against connected runners"
        icon="hero-scale"
      />
      <.data_table
        id="runner-capacity-table"
        rows={@overview.capacity}
        row_testid="runner-capacity-row"
        row_class={fn row -> starved?(row) && "bg-warning/10" end}
      >
        <:col :let={row} label="Pool · release">
          <.stacked_cell
            primary={row.runner_pool}
            secondary={row.required_runner_release_id}
            mono={:secondary}
          />
        </:col>
        <:col :let={row} label="Queued" align={:end}>
          <span class={row.queued_count > 0 && "font-medium text-warning"}>
            {row.queued_count}
          </span>
        </:col>
        <:col :let={row} label="Active" align={:end}>{row.active_count}</:col>
        <:col :let={row} label="Runners" align={:end}>
          <span class={starved?(row) && "font-medium text-warning"}>
            {row.connected_runner_count}
          </span>
        </:col>
        <:col :let={row} label="Oldest wait" align={:end}>
          {wait_label(row.oldest_queued_at, @overview.observed_at)}
        </:col>
      </.data_table>
    </.panel>
    """
  end

  attr :runner, :map, required: true
  attr :current_scope, :any, required: true

  defp runner_card(assigns) do
    ~H"""
    <.list_card data-testid="runner-row">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <.status_badge tone={runner_tone(@runner.status)} label={status_label(@runner.status)} />
          <.mono value={@runner.runner_instance_id} truncate class="max-w-56" />
          <span class="truncate text-sm favn-text-subtle">
            {@runner.runner_pool} · {@runner.required_runner_release_id}
          </span>
        </div>

        <span class="shrink-0 text-sm favn-text-subtle">
          Registered {format_time(@runner.registered_at, @current_scope)}
        </span>
      </div>

      <details class="mt-2">
        <summary class="cursor-pointer text-sm favn-text-subtle">Details</summary>
        <.fact_list
          class="mt-2"
          columns={4}
          facts={[
            %{label: "Task kinds", value: join_values(@runner.supported_task_kinds)},
            %{label: "Capabilities", value: join_values(@runner.capabilities)},
            %{label: "Node", value: @runner.beam_node, mono: true},
            %{label: "Protocol", value: "v#{@runner.protocol_version}"}
          ]}
        />
      </details>

      <p :if={@runner.active_task_id} class="mt-2 text-sm favn-text-muted">
        Active task: <.mono value={@runner.active_task_id} />
      </p>
    </.list_card>
    """
  end

  attr :entry, :map, required: true
  attr :expanded, :map, required: true
  attr :current_scope, :any, required: true

  defp session_entry(%{entry: %{kind: :struggling_group}} = assigns) do
    ~H"""
    <.list_card class="border-warning/35 bg-warning/10" data-testid="runner-struggling-group">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <.status_badge tone={:warning} label="Struggling to start" />
          <span class="text-sm font-medium">
            {@entry.session_count} sessions in {span_label(@entry)}
          </span>
          <span class="truncate text-sm favn-text-subtle">
            {@entry.runner_pool} · {@entry.required_runner_release_id}
          </span>
        </div>

        <span class="shrink-0 text-sm favn-text-subtle">
          {format_time(@entry.first_registered_at, @current_scope)} – {format_time(
            @entry.last_registered_at,
            @current_scope
          )}
        </span>
      </div>

      <p class="mt-2 text-sm favn-text-muted">
        Each session ended shortly after registering, before completing any task. Check the
        runner's startup logs and configuration for this pool and release.
      </p>

      <details class="mt-2">
        <summary class="cursor-pointer text-sm favn-text-subtle">
          Show the {@entry.session_count} sessions
        </summary>
        <.stack gap={:sm} class="mt-2">
          <.session_entry
            :for={session <- @entry.sessions}
            entry={session}
            expanded={@expanded}
            current_scope={@current_scope}
          />
        </.stack>
      </details>
    </.list_card>
    """
  end

  defp session_entry(assigns) do
    assigns =
      assigns
      |> assign(:key, session_key(assigns.entry))
      |> assign(:counts, task_count_segments(assigns.entry.task_counts))

    ~H"""
    <.list_card data-testid="runner-session-row">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <.status_badge tone={session_tone(@entry.state)} label={session_state_label(@entry.state)} />
          <.mono value={@entry.runner_instance_id} truncate class="max-w-56" />
          <span class="truncate text-sm favn-text-subtle">
            {@entry.runner_pool} · {@entry.required_runner_release_id}
          </span>
        </div>

        <span class="shrink-0 text-sm favn-text-subtle" data-testid="runner-session-lifetime">
          Woke {format_time(@entry.registered_at, @current_scope)} · awake {awake_label(@entry)}
          <span :if={@entry.ended_at}>
            · ended {format_time(@entry.ended_at, @current_scope)}
          </span>
        </span>
      </div>

      <div class="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1">
        <.outcome_meter
          :if={@counts != []}
          segments={@counts}
          summary={"#{total_count(@entry.task_counts)} tasks"}
          size={:sm}
          class="min-w-48 max-w-md flex-1"
        />
        <p :if={@counts == []} class="text-sm favn-text-subtle">No tasks completed.</p>

        <p
          :if={interrupted?(@entry)}
          class="text-sm text-warning"
          data-testid="runner-session-interrupted"
        >
          {interrupted_label(@entry)}
        </p>
      </div>

      <div :if={expandable?(@entry)} class="mt-2">
        <.button
          variant={:ghost}
          size={:sm}
          phx-click="toggle_session_tasks"
          phx-value-key={@key}
          phx-value-instance={@entry.runner_instance_id}
          phx-value-generation={@entry.session_generation}
          phx-value-registered-at={DateTime.to_iso8601(@entry.registered_at)}
          phx-value-ended-at={(@entry.ended_at && DateTime.to_iso8601(@entry.ended_at)) || ""}
          data-testid="runner-session-tasks-toggle"
        >
          {(Map.has_key?(@expanded, @key) && "Hide failed tasks") || "Show failed tasks"}
        </.button>

        <.session_tasks
          :if={Map.has_key?(@expanded, @key)}
          tasks={Map.fetch!(@expanded, @key)}
          current_scope={@current_scope}
        />
      </div>
    </.list_card>
    """
  end

  attr :tasks, :any, required: true
  attr :current_scope, :any, required: true

  defp session_tasks(%{tasks: :unavailable} = assigns) do
    ~H"""
    <.notice tone={:error} class="mt-2" data-testid="runner-session-tasks-error">
      <p>Task detail could not be loaded. Retry from the refresh control above.</p>
    </.notice>
    """
  end

  defp session_tasks(assigns) do
    ~H"""
    <.empty_state
      :if={@tasks == []}
      title="No failed tasks from this workspace"
      description="Failures attributed to this session in other workspaces appear in the counts only."
      icon="hero-check-circle"
      data-testid="runner-session-tasks-empty"
    />

    <.stack :if={@tasks != []} gap={:sm} class="mt-2">
      <.task_card :for={task <- @tasks} task={task} current_scope={@current_scope} />
    </.stack>
    """
  end

  attr :task, :map, required: true
  attr :current_scope, :any, required: true

  defp task_card(assigns) do
    ~H"""
    <.list_card data-testid="runner-task-row">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <.stacked_cell
          primary={humanize(@task.task_kind)}
          secondary={@task.task_id}
          mono={:secondary}
        /> <.status_badge tone={task_tone(@task.status)} label={status_label(@task.status)} />
      </div>

      <.notice
        :if={@task.failure}
        class="mt-3"
        tone={:error}
        data-testid="runner-task-error"
      >
        <p class="font-medium">{@task.failure.title}</p>

        <p class="mt-1 break-words">{@task.failure.message}</p>

        <p class="mt-2 favn-text-muted">{@task.failure.remediation}</p>

        <p class="mt-2 font-mono text-sm">{@task.failure.code}</p>
      </.notice>

      <.fact_list
        class="mt-3"
        columns={3}
        facts={[
          %{label: "Run", value: @task.run_id || "-", mono: true},
          %{label: "Queued", value: format_time(@task.enqueued_at, @current_scope)},
          %{label: "Took", value: took_label(@task)}
        ]}
      />
    </.list_card>
    """
  end

  @doc "Primary navigation with the runners destination selected."
  @spec nav_items() :: [Navigation.item()]
  def nav_items, do: Navigation.items(:runners)

  defp subtitle(nil), do: "Live presence, capacity, and session history"
  defp subtitle(%{registry_status: :unavailable}), do: "Live registry unavailable"

  defp subtitle(%{runner_count: count, workspace_tasks: %{queued_count: queued}}) do
    runners = if count == 1, do: "1 runner connected", else: "#{count} runners connected"

    case queued do
      0 -> runners
      1 -> "#{runners} · 1 task waiting"
      queued -> "#{runners} · #{queued} tasks waiting"
    end
  end

  defp starved_partitions(overview) do
    Enum.filter(overview.capacity, &starved?/1)
  end

  defp starved?(row), do: row.queued_count > 0 and row.connected_runner_count == 0

  defp starved_label(row) do
    tasks = if row.queued_count == 1, do: "1 task", else: "#{row.queued_count} tasks"
    "#{tasks} queued for #{row.runner_pool} · #{row.required_runner_release_id}"
  end

  defp state_choices(overview, active) do
    entries = overview.sessions

    counts = %{
      all: length(entries),
      connected: count_state(entries, :connected),
      shut_down: count_state(entries, :shut_down),
      crashed: count_state(entries, :crashed),
      presumed_dead: count_state(entries, :presumed_dead),
      struggling: Enum.count(entries, &(&1.kind == :struggling_group))
    }

    [
      choice(:all, "All", "hero-list-bullet", :neutral, active, counts),
      choice(:connected, "Connected", "hero-signal", :success, active, counts),
      choice(:shut_down, "Shut down", "hero-power", :neutral, active, counts),
      choice(:crashed, "Crashed", "hero-exclamation-triangle", :error, active, counts),
      choice(
        :presumed_dead,
        "Presumed dead",
        "hero-question-mark-circle",
        :warning,
        active,
        counts
      ),
      choice(
        :struggling,
        "Struggling",
        "hero-arrow-path-rounded-square",
        :warning,
        active,
        counts
      )
    ]
  end

  defp choice(id, label, icon, tone, active, counts) do
    %{
      id: Atom.to_string(id),
      label: label,
      icon: icon,
      tone: tone,
      active?: active == id,
      count: Map.fetch!(counts, id),
      count_label: "sessions"
    }
  end

  defp count_state(entries, state),
    do: Enum.count(entries, &(&1.kind == :session and &1.state == state))

  defp window_choices(active) do
    for {id, label} <- [today: "Today", week: "7 days", month: "30 days", all: "All"] do
      %{
        id: Atom.to_string(id),
        label: label,
        icon: "hero-calendar",
        tone: :neutral,
        active?: active == id,
        count: nil
      }
    end
  end

  defp displayed_sessions(overview, :all), do: overview.sessions

  defp displayed_sessions(overview, :struggling),
    do: Enum.filter(overview.sessions, &(&1.kind == :struggling_group))

  defp displayed_sessions(overview, state),
    do: Enum.filter(overview.sessions, &(&1.kind == :session and &1.state == state))

  defp session_key(entry) do
    "#{entry.runner_instance_id}:#{entry.session_generation}:#{DateTime.to_iso8601(entry.registered_at)}"
  end

  defp session_tone(:connected), do: :success
  defp session_tone(:shut_down), do: :neutral
  defp session_tone(:crashed), do: :error
  defp session_tone(:presumed_dead), do: :warning

  defp session_state_label(:connected), do: "Connected"
  defp session_state_label(:shut_down), do: "Shut down"
  defp session_state_label(:crashed), do: "Crashed"
  defp session_state_label(:presumed_dead), do: "Presumed dead"

  defp task_count_segments(task_counts) do
    [
      %{tone: :success, count: Map.get(task_counts, :succeeded, 0), label: "succeeded"},
      %{tone: :error, count: Map.get(task_counts, :failed, 0), label: "failed"},
      %{tone: :warning, count: Map.get(task_counts, :unknown, 0), label: "unknown"},
      %{tone: :neutral, count: Map.get(task_counts, :cancelled, 0), label: "cancelled"}
    ]
    |> Enum.filter(&(&1.count > 0))
  end

  defp total_count(task_counts), do: task_counts |> Map.values() |> Enum.sum()

  defp interrupted?(entry), do: entry.busy_at_exit and entry.state != :connected

  defp interrupted_label(%{interrupted_task_id: task_id}) when is_binary(task_id),
    do: "1 task interrupted (#{task_id}) — outcome unknown."

  defp interrupted_label(_entry),
    do: "1 task interrupted in another workspace — outcome unknown."

  defp expandable?(entry) do
    Map.get(entry.task_counts, :failed, 0) > 0 or Map.get(entry.task_counts, :unknown, 0) > 0 or
      interrupted?(entry)
  end

  defp awake_label(%{ended_at: nil, registered_at: registered_at}),
    do: duration_label(DateTime.diff(DateTime.utc_now(), registered_at, :millisecond))

  defp awake_label(%{ended_at: ended_at, registered_at: registered_at}),
    do: duration_label(DateTime.diff(ended_at, registered_at, :millisecond))

  defp span_label(entry) do
    duration_label(
      DateTime.diff(entry.last_registered_at, entry.first_registered_at, :millisecond)
    )
  end

  defp totals_label(totals, window) do
    busy_share =
      if totals.awake_ms > 0,
        do: " (#{round(totals.busy_ms * 100 / totals.awake_ms)}% of awake time)",
        else: ""

    "#{totals.session_count} sessions #{window_phrase(window)} · awake #{duration_label(totals.awake_ms)}" <>
      " · busy #{duration_label(totals.busy_ms)}#{busy_share} · idle #{duration_label(totals.idle_ms)}." <>
      " Busy time covers completed final assignments."
  end

  defp window_phrase(:today), do: "today"
  defp window_phrase(:week), do: "in the last 7 days"
  defp window_phrase(:month), do: "in the last 30 days"
  defp window_phrase(:all), do: "in the last 30 days (totals window)"

  defp failed_window_label(:today), do: "today"
  defp failed_window_label(:week), do: "7 d"
  defp failed_window_label(:month), do: "30 d"
  defp failed_window_label(:all), do: "24 h"

  defp wait_label(nil, _now), do: "-"

  defp wait_label(%DateTime{} = oldest, %DateTime{} = now),
    do: duration_label(DateTime.diff(now, oldest, :millisecond))

  defp took_label(%{
         assigned_at: %DateTime{} = assigned_at,
         terminal_at: %DateTime{} = terminal_at
       }),
       do: duration_label(DateTime.diff(terminal_at, assigned_at, :millisecond))

  defp took_label(_task), do: "-"

  defp duration_label(ms) when is_integer(ms) and ms < 0, do: "-"
  defp duration_label(ms) when ms < 1_000, do: "#{ms} ms"
  defp duration_label(ms) when ms < 60_000, do: "#{div(ms, 1_000)} s"

  defp duration_label(ms) when ms < 3_600_000 do
    minutes = div(ms, 60_000)
    seconds = ms |> rem(60_000) |> div(1_000)
    if seconds == 0, do: "#{minutes} m", else: "#{minutes} m #{seconds} s"
  end

  defp duration_label(ms) when ms < 86_400_000 do
    hours = div(ms, 3_600_000)
    minutes = ms |> rem(3_600_000) |> div(60_000)
    if minutes == 0, do: "#{hours} h", else: "#{hours} h #{minutes} m"
  end

  defp duration_label(ms) do
    days = div(ms, 86_400_000)
    hours = ms |> rem(86_400_000) |> div(3_600_000)
    if hours == 0, do: "#{days} d", else: "#{days} d #{hours} h"
  end

  defp runner_tone(status) when status in [:idle], do: :success
  defp runner_tone(status) when status in [:claiming, :reserved, :busy], do: :info
  defp runner_tone(:draining), do: :warning
  defp runner_tone(_status), do: :neutral

  defp task_tone(:succeeded), do: :success
  defp task_tone(status) when status in [:failed, :unknown], do: :error
  defp task_tone(status) when status in [:assigned, :preparing, :running, :cancelling], do: :info
  defp task_tone(:cancelled), do: :warning
  defp task_tone(_status), do: :neutral

  defp status_label(status), do: humanize(status)

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp join_values([]), do: "None"
  defp join_values(values), do: Enum.map_join(values, ", ", &to_string/1)

  defp format_time(nil, _timezone), do: "-"

  defp format_time(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %H:%M:%S", timezone)
end
