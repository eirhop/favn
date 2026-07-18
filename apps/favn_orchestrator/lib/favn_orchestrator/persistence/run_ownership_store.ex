defmodule FavnOrchestrator.Persistence.RunOwnershipStore do
  @moduledoc "Persistence contract for multi-node run ownership and durable runner execution."

  alias FavnOrchestrator.Persistence.Commands.AdvanceRunnerExecution
  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.RecordRunnerDispatch
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageRunnerExecutions
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RunnerExecution
  alias FavnOrchestrator.Persistence.Results.RunOwnership

  @callback claim_run(ClaimRun.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback claim_recovery_batch(ClaimRecoveryBatch.t()) ::
              {:ok, [RunOwnership.t()]} | {:error, Error.t()}
  @callback renew_run(RenewRunOwnership.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback release_run(ReleaseRunOwnership.t()) :: :ok | {:error, Error.t()}
  @callback record_dispatch(RecordRunnerDispatch.t()) ::
              {:ok, RunnerExecution.t()} | {:error, Error.t()}
  @callback advance_execution(AdvanceRunnerExecution.t()) ::
              {:ok, RunnerExecution.t()} | {:error, Error.t()}
  @callback page_executions(PageRunnerExecutions.t()) ::
              {:ok, CursorPage.t(RunnerExecution.t())} | {:error, Error.t()}
end
