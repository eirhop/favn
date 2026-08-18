defmodule FavnOrchestrator.RunServer.Recovery do
  @moduledoc """
  Decides whether a persisted run has enough durable position to resume safely.

  Recovery is deliberately fail-closed. A retry checkpoint is resumable, as is a
  fresh run that has not submitted a runner task. Other histories may contain a
  completed effect whose continuation position is not durable, so they must be
  terminalized instead of resubmitted.
  """

  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.RecoveryPosition
  alias FavnOrchestrator.RunServer.RetryCheckpoint
  alias FavnOrchestrator.RunState

  @type disposition :: :resume | {:uncertain, map()}

  @doc "Loads bounded durable runner-execution evidence and assesses recovery safety."
  @spec disposition(RunState.t()) :: {:ok, disposition()} | {:error, term()}
  def disposition(%RunState{} = run) do
    case ActiveTaskSet.active_runner_task_ids(run) do
      [] ->
        {:ok, assess_checkpoint(run)}

      task_ids ->
        if RecoveryPosition.outcome_recorded?(run) do
          {:ok, uncertain(:active_stage_outcomes_not_resumable, task_ids, false)}
        else
          validate_durable_tasks(run, task_ids)
        end
    end
  end

  defp validate_durable_tasks(run, task_ids) do
    Enum.reduce_while(task_ids, {:ok, []}, fn task_id, {:ok, missing_ids} ->
      case RunnerTasks.fetch(run.workspace_id, task_id) do
        {:ok, _task} -> {:cont, {:ok, missing_ids}}
        {:error, %{kind: :not_found}} -> {:cont, {:ok, [task_id | missing_ids]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, []} ->
        {:ok, :resume}

      {:ok, missing_ids} ->
        {:ok,
         {:uncertain,
          %{
            reason: :durable_runner_tasks_missing,
            missing_task_ids: Enum.sort(missing_ids),
            truncated?: false
          }}}

      error ->
        error
    end
  end

  defp assess_checkpoint(%RunState{} = run) do
    case RetryCheckpoint.validate(run.metadata, RunState.execution_mode(run)) do
      {:ok, {kind, _state}} when kind in [:sequential, :pipeline] ->
        :resume

      {:ok, :none} when run.event_seq <= 2 ->
        :resume

      {:ok, :none} ->
        uncertain(:continuation_position_not_durable, [], false)

      {:error, :invalid_retry_checkpoint} ->
        uncertain(:invalid_retry_checkpoint, [], false)
    end
  end

  defp uncertain(reason, active, truncated?) do
    {:uncertain,
     %{
       reason: reason,
       active_runner_task_count: length(active),
       runner_tasks: Enum.sort(active),
       truncated?: truncated?
     }}
  end
end
