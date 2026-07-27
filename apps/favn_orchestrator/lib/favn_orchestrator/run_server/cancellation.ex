defmodule FavnOrchestrator.RunServer.Cancellation do
  @moduledoc """
  Shared runner cancellation payload and dispatch helpers for run-server paths.

  This module owns the runner cancellation envelope contract only. Callers remain
  responsible for any local run-state cleanup after dispatching cancellation.
  """

  alias Favn.Contracts.RunnerCancellation
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunState

  @type execution_id :: String.t()
  @type reason :: term()
  @doc "Requests durable runner-task cancellation and returns one outcome per task."
  @spec dispatch_runner_tasks(RunState.t(), [term()], reason()) :: [
          CancellationOutcome.t()
        ]
  def dispatch_runner_tasks(%RunState{} = run_state, task_ids, reason) when is_list(task_ids) do
    safe_reason = Redaction.redact_operational_bounded(%{reason: reason}).reason

    task_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.map(&RunnerTasks.request_cancellation(run_state.workspace_id, &1, safe_reason))
  end

  @doc "Legacy cancellation seam for non-asset runner operations not migrated to tasks yet."
  @spec dispatch_legacy_runner_work(RunState.t(), [term()], reason(), module(), keyword()) :: [
          CancellationOutcome.t()
        ]
  def dispatch_legacy_runner_work(run_state, execution_ids, reason, runner_client, runner_opts) do
    envelope =
      RunnerCancellation.request(
        run_state.id,
        Redaction.redact_operational_bounded(%{reason: reason}).reason
      )

    execution_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.map(fn execution_id ->
      result =
        try do
          runner_client.cancel_work(execution_id, envelope, runner_opts)
        rescue
          exception -> {:error, exception}
        catch
          kind, caught -> {:error, {kind, caught}}
        end

      CancellationOutcome.from_runner_result(execution_id, result)
    end)
  end
end
