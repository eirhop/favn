defmodule FavnOrchestrator.Persistence.RunStore do
  @moduledoc "Persistence contract for authoritative run snapshots, events, targets, and input pins."

  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Commands.PutRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.RunState

  alias FavnOrchestrator.Persistence.Queries.PageCancellingOperations
  alias FavnOrchestrator.Persistence.Results.{CancellationScope, CancellationWork}

  @callback cancellation_scope(GetRun.t()) :: {:ok, CancellationScope.t()} | {:error, Error.t()}
  @callback request_operation_cancellation(RequestRunCancellation.t()) ::
              :ok | {:error, Error.t()}
  @callback page_cancelling_operations(PageCancellingOperations.t()) ::
              {:ok, CursorPage.t(String.t())} | {:error, Error.t()}
  @callback reconcile_cancellation(GetRun.t()) ::
              {:ok, CancellationWork.t()} | {:error, Error.t()}

  @callback create_run(CreateRun.t()) :: {:ok, RunCommitted.t()} | {:error, Error.t()}
  @callback commit_transition(CommitRunTransition.t()) ::
              {:ok, RunCommitted.t()} | {:error, Error.t()}
  @callback request_cancellation(RequestRunCancellation.t()) ::
              {:ok, RunCommitted.t()} | {:error, Error.t()}
  @callback get_run(GetRun.t()) :: {:ok, RunState.t()} | {:error, Error.t()}
  @callback page_runs(PageRuns.t()) :: {:ok, CursorPage.t(RunState.t())} | {:error, Error.t()}
  @callback page_run_summaries(PageRuns.t()) ::
              {:ok, CursorPage.t(RunSummary.t())} | {:error, Error.t()}
  @callback page_events(PageRunEvents.t() | PagePublishedRunEvents.t()) ::
              {:ok, CursorPage.t(map())} | {:error, Error.t()}
  @callback pin_runtime_inputs(PinRuntimeInputs.t()) :: {:ok, [Pin.t()]} | {:error, Error.t()}
  @callback get_runtime_inputs(GetRuntimeInputs.t()) :: {:ok, [Pin.t()]} | {:error, Error.t()}
  @callback put_execution_checkpoint(PutRunExecutionCheckpoint.t()) ::
              {:ok, RunExecutionCheckpoint.t()} | {:error, Error.t()}
  @callback get_execution_checkpoint(GetRunExecutionCheckpoint.t()) ::
              {:ok, RunExecutionCheckpoint.t()} | {:error, Error.t()}
end
