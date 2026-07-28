defmodule FavnOrchestrator.RunnerTaskRecovery do
  @moduledoc "Bounded recovery loop for expired fenced runner-task assignments."
  use GenServer

  alias Favn.Contracts.RunnerError
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerTaskResultRouter

  @default_interval_ms 5_000
  @default_lease_ms 30_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lease_ms: Keyword.get(opts, :lease_ms, @default_lease_ms),
      owner_id: "recovery:#{FavnOrchestrator.RuntimeConfig.instance_id()}"
    }

    schedule(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:recover, state) do
    recover(state)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  def handle_info({:runner_down, _runner_id, _generation, _reason}, state) do
    schedule(state.lease_ms)
    {:noreply, state}
  end

  defp recover(state) do
    now = DateTime.utc_now()
    suffix = System.unique_integer([:positive, :monotonic])

    command = %C.RecoverRunnerTasks{
      platform_context:
        SystemContext.platform(:runner_task_recovery, roles: [:platform_operator]),
      command_id: "recover:#{suffix}",
      owner_id: state.owner_id,
      issued_at: now,
      occurred_at: now,
      limit: 50,
      lease_duration_ms: state.lease_ms
    }

    case Persistence.stores().runner_tasks.recover_expired(command) do
      {:ok, tasks} -> Enum.each(tasks, &recover_task(&1, state, now))
      {:error, _reason} -> :ok
    end
  end

  defp recover_task(task, state, now) do
    disposition = recovery_disposition(task)

    reason =
      case disposition do
        :requeue ->
          RunnerError.new(outcome: :safe_failure, retryable?: true)

        :cancelled ->
          RunnerError.cancelled(:runner_lost_after_cancellation_ack)

        :unknown ->
          RunnerError.new(outcome: :unknown, retryable?: false)
      end

    command = %C.ReleaseRunnerTask{
      workspace_context:
        SystemContext.workspace(task.workspace_id, :runner_task_recovery_release),
      command_id: "recover-release:#{task.task_id}:#{task.assignment_generation}",
      task_id: task.task_id,
      runner_instance_id: state.owner_id,
      runner_session_generation: 0,
      assignment_generation: task.assignment_generation,
      disposition: disposition,
      reason: reason,
      issued_at: now,
      occurred_at: now
    }

    case Persistence.stores().runner_tasks.release(command) do
      {:ok, %{status: :queued} = released} ->
        RunnerQueueCoordinator.notify(
          released.runner_pool,
          released.required_runner_release_id,
          1
        )

      {:ok, %{status: status} = released}
      when status in [:cancelled, :unknown] ->
        RunnerTaskResultRouter.notify(released)

      _other ->
        :ok
    end
  end

  @doc false
  @spec recovery_disposition(map()) :: :cancelled | :requeue | :unknown
  def recovery_disposition(%{
        status: :cancelling,
        cancellation_acknowledged_at: %DateTime{},
        retry_class: :safe_to_retry
      }),
      do: :cancelled

  def recovery_disposition(task) do
    if safe_to_requeue?(task), do: :requeue, else: :unknown
  end

  defp safe_to_requeue?(%{status: status}) when status in [:assigned, :preparing], do: true

  defp safe_to_requeue?(%{status: :running, retry_class: :safe_to_retry}),
    do: true

  defp safe_to_requeue?(_task), do: false

  defp schedule(delay), do: Process.send_after(self(), :recover, delay)
end
