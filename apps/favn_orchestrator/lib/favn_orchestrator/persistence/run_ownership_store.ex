defmodule FavnOrchestrator.Persistence.RunOwnershipStore do
  @moduledoc "Persistence contract for fenced multi-node run ownership."

  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership

  @callback check_resume(FavnOrchestrator.Persistence.Commands.ResumeRunRecovery.t()) ::
              {:ok, :ready | :already_resumed} | {:error, Error.t()}

  @callback require_diagnosis(FavnOrchestrator.Persistence.Commands.RequireRunDiagnosis.t()) ::
              :ok | {:error, Error.t()}

  @callback resume_recovery(FavnOrchestrator.Persistence.Commands.ResumeRunRecovery.t()) ::
              :ok | {:error, Error.t()}

  @callback maintain_targets(
              FavnOrchestrator.Persistence.WorkspaceContext.t(),
              RunOwnership.t(),
              [String.t()],
              String.t()
            ) :: {:ok, map()} | {:error, Error.t()}

  @callback recovery_candidates(FavnOrchestrator.Persistence.WorkspaceContext.t(), pos_integer()) ::
              {:ok, [String.t()]} | {:error, Error.t()}

  @callback cleanup_candidates(FavnOrchestrator.Persistence.WorkspaceContext.t(), pos_integer()) ::
              {:ok, [String.t()]} | {:error, Error.t()}

  @callback claim_run(ClaimRun.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback claim_recovery_batch(ClaimRecoveryBatch.t()) ::
              {:ok, [RunOwnership.t()]} | {:error, Error.t()}
  @callback renew_run(RenewRunOwnership.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback release_run(ReleaseRunOwnership.t()) :: :ok | {:error, Error.t()}
end
