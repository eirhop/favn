defmodule FavnOrchestrator.RunServer.Cancellation do
  @moduledoc """
  Shared runner cancellation payload and dispatch helpers for run-server paths.

  This module owns the runner cancellation envelope contract only. Callers remain
  responsible for any local run-state cleanup after dispatching cancellation.
  """

  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunState

  @type task_id :: String.t()
  @type reason :: term()
  @doc "Requests durable runner-task cancellation and returns one outcome per task."
  @spec dispatch_runner_tasks(RunState.t(), [term()], reason(), keyword()) :: [
          CancellationOutcome.t()
        ]
  def dispatch_runner_tasks(%RunState{} = run_state, task_ids, reason, opts \\ [])
      when is_list(task_ids) do
    safe_reason = Redaction.redact_operational_bounded(%{reason: reason}).reason

    task_ids
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.map(&RunnerTasks.request_cancellation(run_state.workspace_id, &1, safe_reason, opts))
  end
end
