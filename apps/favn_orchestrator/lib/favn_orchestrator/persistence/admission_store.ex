defmodule FavnOrchestrator.Persistence.AdmissionStore do
  @moduledoc "Persistence contract for distributed capacity admission and waiters."

  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.ClaimAdmissionWaiters
  alias FavnOrchestrator.Persistence.Commands.ExpireAdmission
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunLeases
  alias FavnOrchestrator.Persistence.Commands.RenewExecutionLease
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Admission
  alias FavnOrchestrator.Persistence.Results.AdmissionWaiter
  alias FavnOrchestrator.Persistence.Results.CapacityRelease
  alias FavnOrchestrator.Persistence.Results.ExecutionLease

  @callback admit(AdmitExecution.t()) :: {:ok, Admission.t()} | {:error, Error.t()}
  @callback renew_lease(RenewExecutionLease.t()) ::
              {:ok, ExecutionLease.t()} | {:error, Error.t()}
  @callback release_lease(ReleaseExecutionLease.t()) ::
              {:ok, CapacityRelease.t()} | {:error, Error.t()}
  @callback release_run_leases(ReleaseRunLeases.t()) ::
              {:ok, CapacityRelease.t()} | {:error, Error.t()}
  @callback claim_waiters(ClaimAdmissionWaiters.t()) ::
              {:ok, [AdmissionWaiter.t()]} | {:error, Error.t()}
  @callback expire(ExpireAdmission.t()) ::
              {:ok, CapacityRelease.t()} | {:error, Error.t()}
end
