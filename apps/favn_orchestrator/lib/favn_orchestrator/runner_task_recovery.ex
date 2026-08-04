defmodule FavnOrchestrator.RunnerTaskRecovery do
  @moduledoc """
  Bounded recovery loop for expired fenced runner-task assignments.

  Every claim, retry, and recovery fence increments a task's assignment
  generation, so the generation counts every fencing event in the task's
  lifetime - an expire-and-requeue cycle consumes two units (the recovery
  fence plus the next claim). A task whose generation reaches the assignment
  budget is released as terminal `:unknown` instead of requeued: without the
  budget, a task that poisons its runner on every assignment would monopolize
  its pool forever. The default budget of 12 parks such a task after roughly
  six expire-and-requeue cycles.
  """
  use GenServer

  alias Favn.Contracts.RunnerError
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerTaskResultRouter

  @default_interval_ms 5_000
  @default_lease_ms 30_000
  @default_assignment_budget 12

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      lease_ms: Keyword.get(opts, :lease_ms, @default_lease_ms),
      assignment_budget: Keyword.get(opts, :assignment_budget, @default_assignment_budget),
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
    disposition = recovery_disposition(task, state.assignment_budget)

    reason =
      case disposition do
        :requeue ->
          RunnerError.new(outcome: :safe_failure, retryable?: true)

        :cancelled ->
          RunnerError.cancelled(:runner_lost_after_cancellation_ack)

        :unknown ->
          unknown_recovery_reason(task, state.assignment_budget)
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
  @spec recovery_disposition(map(), pos_integer()) :: :cancelled | :requeue | :unknown
  def recovery_disposition(task, assignment_budget \\ @default_assignment_budget)

  def recovery_disposition(
        %{
          status: :cancelling,
          cancellation_acknowledged_at: %DateTime{},
          retry_class: :safe_to_retry
        },
        _assignment_budget
      ),
      do: :cancelled

  def recovery_disposition(task, assignment_budget) do
    cond do
      assignment_budget_exhausted?(task, assignment_budget) -> :unknown
      safe_to_requeue?(task) -> :requeue
      true -> :unknown
    end
  end

  @doc false
  @spec assignment_budget_exhausted?(map(), pos_integer()) :: boolean()
  def assignment_budget_exhausted?(%{assignment_generation: generation}, assignment_budget)
      when is_integer(generation) and is_integer(assignment_budget),
      do: generation >= assignment_budget

  def assignment_budget_exhausted?(_task, _assignment_budget), do: false

  @doc false
  @spec unknown_recovery_reason(map(), pos_integer()) :: RunnerError.t()
  def unknown_recovery_reason(task, assignment_budget) do
    if assignment_budget_exhausted?(task, assignment_budget) do
      RunnerError.new(
        type: :runner_task_assignment_budget_exhausted,
        message:
          "Runner task exhausted its assignment budget after repeated lease expiries " <>
            "and was released as unknown",
        details: %{
          assignment_generation: Map.get(task, :assignment_generation),
          assignment_budget: assignment_budget
        },
        retryable?: false,
        outcome: :unknown
      )
    else
      RunnerError.new(outcome: :unknown, retryable?: false)
    end
  end

  defp safe_to_requeue?(%{status: status}) when status in [:assigned, :preparing], do: true

  defp safe_to_requeue?(%{status: :running, retry_class: :safe_to_retry}),
    do: true

  defp safe_to_requeue?(_task), do: false

  defp schedule(delay), do: Process.send_after(self(), :recover, delay)
end
