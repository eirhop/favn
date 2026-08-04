defmodule FavnOrchestrator.Persistence.RunnerTaskStore do
  @moduledoc """
  Persistence contract for durable, fenced runner tasks and exact capacity
  demand.

  Error kinds carry retry semantics the runner acts on, so implementations
  must keep them apart: a command whose fencing identity (instance id, session
  generation, assignment generation) no longer matches the current assignment
  fails with kind `:fenced`, while a content conflict from a correctly fenced
  caller - a payload mismatch, an already-recorded resolution - fails with
  kind `:conflict`. The runner abandons an assignment on `:fenced` and treats
  `:conflict` as a permanent rejection of the message it sent; conflating the
  two turns one into the other's recovery path.
  """

  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.Persistence.Queries, as: Q
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityHealth
  alias FavnOrchestrator.Persistence.Results.RunnerReleaseDrain
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Error

  @callback enqueue(C.EnqueueRunnerTask.t()) :: {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback claim(C.ClaimRunnerTask.t()) ::
              {:ok, RunnerTask.t() | nil} | {:error, Error.t()}
  @callback transition(C.TransitionRunnerTask.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback persist_runtime_inputs(C.PersistRunnerTaskRuntimeInputs.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback append_log_batch(C.AppendRunnerTaskLogBatch.t()) ::
              {:ok, :persisted | :already_persisted} | {:error, Error.t()}
  @callback complete(C.CompleteRunnerTask.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback request_cancellation(C.RequestRunnerTaskCancellation.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback acknowledge_cancellation(C.AcknowledgeRunnerTaskCancellation.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback release(C.ReleaseRunnerTask.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback retry(C.RetryRunnerTask.t()) ::
              {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback recover_expired(C.RecoverRunnerTasks.t()) ::
              {:ok, [RunnerTask.t()]} | {:error, Error.t()}
  @callback reconcile_demand(C.ReconcileRunnerCapacityDemand.t()) ::
              {:ok, RunnerCapacityDemand.t()} | {:error, Error.t()}
  @callback ensure_demand(C.EnsureRunnerCapacityDemand.t()) ::
              {:ok, RunnerCapacityDemand.t()} | {:error, Error.t()}
  @callback get(Q.GetRunnerTask.t()) :: {:ok, RunnerTask.t()} | {:error, Error.t()}
  @callback page_run(Q.PageRunRunnerTasks.t()) ::
              {:ok, [RunnerTask.t()]} | {:error, Error.t()}
  @callback page_workspace(Q.PageWorkspaceRunnerTasks.t()) ::
              {:ok, [RunnerTask.t()]} | {:error, Error.t()}
  @callback demand(Q.GetRunnerCapacityDemand.t()) ::
              {:ok, RunnerCapacityDemand.t()} | {:error, Error.t()}
  @callback list_demands(Q.ListRunnerCapacityDemands.t()) ::
              {:ok, [RunnerCapacityDemand.t()]} | {:error, Error.t()}
  @callback release_drain(Q.GetRunnerReleaseDrain.t()) ::
              {:ok, RunnerReleaseDrain.t()} | {:error, Error.t()}
  @callback capacity_health(Q.GetRunnerCapacityHealth.t()) ::
              {:ok, RunnerCapacityHealth.t()} | {:error, Error.t()}
  @callback list_release_drains(Q.ListRunnerReleaseDrains.t()) ::
              {:ok, [RunnerReleaseDrain.t()]} | {:error, Error.t()}
end
