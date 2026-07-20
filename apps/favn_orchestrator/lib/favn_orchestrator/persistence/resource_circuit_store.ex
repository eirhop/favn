defmodule FavnOrchestrator.Persistence.ResourceCircuitStore do
  @moduledoc "Persistence contract for durable shared-resource circuits and recovery candidates."

  alias FavnOrchestrator.Persistence.Commands.AcquireResourceCircuits
  alias FavnOrchestrator.Persistence.Commands.ClaimResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.CompleteResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.ListPendingResourceRecoveries
  alias FavnOrchestrator.Persistence.Commands.RecordResourceOutcomes
  alias FavnOrchestrator.Persistence.Commands.RecordResourceRecoveryCandidate
  alias FavnOrchestrator.Persistence.Commands.ReleaseResourceCircuitPermits
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryBatch
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryWakeup

  @callback acquire(AcquireResourceCircuits.t()) ::
              {:ok, ResourceCircuitAdmission.t()} | {:error, Error.t()}
  @callback record_outcomes(RecordResourceOutcomes.t()) ::
              {:ok, ResourceCircuitUpdate.t()} | {:error, Error.t()}
  @callback record_recovery_candidate(RecordResourceRecoveryCandidate.t()) ::
              :ok | {:error, Error.t()}
  @callback release_permits(ReleaseResourceCircuitPermits.t()) :: :ok | {:error, Error.t()}
  @callback claim_recovery(ClaimResourceRecovery.t()) ::
              {:ok, ResourceRecoveryBatch.t()} | {:error, Error.t()}
  @callback complete_recovery(CompleteResourceRecovery.t()) :: :ok | {:error, Error.t()}
  @callback list_pending_recoveries(ListPendingResourceRecoveries.t()) ::
              {:ok, [ResourceRecoveryWakeup.t()]} | {:error, Error.t()}
end
