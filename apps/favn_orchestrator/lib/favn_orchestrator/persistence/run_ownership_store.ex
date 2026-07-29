defmodule FavnOrchestrator.Persistence.RunOwnershipStore do
  @moduledoc "Persistence contract for fenced multi-node run ownership."

  alias FavnOrchestrator.Persistence.Commands.ClaimRecoveryBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunOwnership

  @callback claim_run(ClaimRun.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback claim_recovery_batch(ClaimRecoveryBatch.t()) ::
              {:ok, [RunOwnership.t()]} | {:error, Error.t()}
  @callback renew_run(RenewRunOwnership.t()) :: {:ok, RunOwnership.t()} | {:error, Error.t()}
  @callback release_run(ReleaseRunOwnership.t()) :: :ok | {:error, Error.t()}
end
