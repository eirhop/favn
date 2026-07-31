defmodule FavnView.Components.RunnersPage do
  @moduledoc """
  Runner operations page: current live presence and durable recent task failures.

  A disconnected runner disappears from the live section, but its task failures
  remain visible because the activity section is backed by PostgreSQL.
  """

  use FavnView, :html

  alias FavnView.Components.AppShell
  alias FavnView.Components.Navigation

  attr :overview, :map, default: nil
  attr :loading, :boolean, default: false
  attr :error, :string, default: nil
  attr :nav_items, :list, required: true
  attr :flash, :map, default: %{}

  def runners_page(assigns) do
    ~H"""
    <AppShell.app_shell
      title="Runners"
      subtitle={subtitle(@overview)}
      nav_items={@nav_items}
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

        <.overview :if={!@loading && !@error && @overview} overview={@overview} />
      </div>
    </AppShell.app_shell>
    """
  end

  attr :overview, :map, required: true

  defp overview(assigns) do
    ~H"""
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
          Live runner presence cannot be read on this control-plane node. Durable task history remains available below.
        </p>
      </.notice>

      <.stack :if={@overview.runners != []} gap={:sm}>
        <.runner_card :for={runner <- @overview.runners} runner={runner} />
      </.stack>
    </.panel>

    <.panel data-testid="runner-failures">
      <:header
        title="Recent failures"
        subtitle="Durable workspace failures remain visible even after newer work arrives"
        icon="hero-exclamation-triangle"
      />

      <.empty_state
        :if={failures(@overview) == []}
        title="No recent runner failures"
        description="Failed and unknown runner tasks will appear here."
        icon="hero-check-circle"
        data-testid="runner-failures-empty-state"
      />

      <.stack :if={failures(@overview) != []} gap={:sm}>
        <.task_card :for={task <- failures(@overview)} task={task} />
      </.stack>
    </.panel>

    <.panel data-testid="runner-task-activity">
      <:header
        title="Recent runner activity"
        subtitle="Durable workspace task history, newest first"
        icon="hero-queue-list"
      />

      <.empty_state
        :if={activity_tasks(@overview) == []}
        title="No other recent runner activity"
        description="Queued and completed task activity will appear here."
        icon="hero-queue-list"
        data-testid="runner-tasks-empty-state"
      />

      <.stack :if={activity_tasks(@overview) != []} gap={:sm}>
        <.task_card :for={task <- activity_tasks(@overview)} task={task} />
      </.stack>
    </.panel>
    """
  end

  attr :runner, :map, required: true

  defp runner_card(assigns) do
    ~H"""
    <.list_card data-testid="runner-row">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <.stacked_cell
          primary={@runner.runner_instance_id}
          secondary={"#{@runner.runner_pool} · #{@runner.required_runner_release_id}"}
          mono={:both}
        />
        <.status_badge tone={runner_tone(@runner.status)} label={status_label(@runner.status)} />
      </div>
      <.fact_list
        class="mt-3"
        columns={3}
        facts={[
          %{label: "Task kinds", value: join_values(@runner.supported_task_kinds)},
          %{label: "Capabilities", value: join_values(@runner.capabilities)},
          %{label: "Registered", value: format_time(@runner.registered_at)}
        ]}
      />
      <p :if={@runner.active_task_id} class="mt-3 text-xs favn-text-muted">
        Active task: <.mono value={@runner.active_task_id} />
      </p>
    </.list_card>
    """
  end

  attr :task, :map, required: true

  defp task_card(assigns) do
    ~H"""
    <.list_card data-testid="runner-task-row">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <.stacked_cell
          primary={humanize(@task.task_kind)}
          secondary={@task.task_id}
          mono={:secondary}
        />
        <.status_badge tone={task_tone(@task.status)} label={status_label(@task.status)} />
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
        <p class="mt-2 font-mono text-xs">{@task.failure.code}</p>
      </.notice>

      <.fact_list
        class="mt-3"
        columns={3}
        facts={[
          %{label: "Pool", value: @task.runner_pool},
          %{label: "Runner", value: @task.assigned_runner_instance_id || "Not assigned", mono: true},
          %{label: "Queued", value: format_time(@task.enqueued_at)}
        ]}
      />
    </.list_card>
    """
  end

  @doc "Primary navigation with the runners destination selected."
  @spec nav_items() :: [Navigation.item()]
  def nav_items, do: Navigation.items(:runners)

  defp subtitle(nil), do: "Live presence and durable task diagnostics"
  defp subtitle(%{registry_status: :unavailable}), do: "Live registry unavailable"
  defp subtitle(%{runner_count: 1}), do: "1 runner connected"
  defp subtitle(%{runner_count: count}), do: "#{count} runners connected"

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

  defp failures(overview), do: Map.get(overview, :failures, [])

  defp activity_tasks(overview) do
    failure_ids = overview |> failures() |> MapSet.new(& &1.task_id)
    Enum.reject(overview.tasks, &MapSet.member?(failure_ids, &1.task_id))
  end

  defp format_time(nil), do: "-"

  defp format_time(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")
end
