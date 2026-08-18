defmodule FavnOrchestrator.RunExecutionCleanup do
  @moduledoc false

  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunState

  @spec cancel_active(RunState.t(), term()) :: [map()]
  def cancel_active(%RunState{} = run, reason) do
    run
    |> ActiveTaskSet.active_runner_task_ids()
    |> then(&Cancellation.dispatch_runner_tasks(run, &1, reason))
    |> Enum.map(&status/1)
  end

  @spec confirmed?([map()]) :: boolean()
  def confirmed?(statuses) when is_list(statuses) do
    Enum.all?(statuses, &(Map.get(&1, :status) in [:acknowledged, :already_completed]))
  end

  @spec release_admission(RunState.t()) :: :ok
  def release_admission(%RunState{} = run) do
    case ActiveTaskSet.active_runner_task_ids(run) do
      [] ->
        release_run_admission(run)

      task_ids ->
        OperationalEvents.emit(
          :run_execution_admission_cleanup_deferred,
          %{active_runner_task_count: length(task_ids)},
          %{run_id: run.id},
          level: :warning
        )

        :ok
    end
  end

  defp release_run_admission(run) do
    case ExecutionAdmission.release_run(run) do
      :ok ->
        :ok

      {:error, reason} ->
        OperationalEvents.emit(
          :run_execution_admission_cleanup_failed,
          %{},
          %{run_id: run.id, reason: reason},
          level: :warning
        )

        :ok
    end
  end

  defp status(%CancellationOutcome{} = outcome) do
    %{
      runner_task_id: outcome.task_id,
      status: outcome.status,
      error: safe_error(outcome.error)
    }
  end

  defp status(outcome) when is_map(outcome) do
    %{
      runner_task_id: Map.get(outcome, :task_id),
      status: Map.get(outcome, :status),
      error: safe_error(Map.get(outcome, :error))
    }
  end

  defp safe_error(nil), do: nil

  defp safe_error(error) do
    case Redaction.redact_operational_bounded(%{error: error}) do
      %{error: safe} -> safe
      _other -> "[REDACTED]"
    end
  end
end
