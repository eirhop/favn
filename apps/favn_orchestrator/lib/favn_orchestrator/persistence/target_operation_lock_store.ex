defmodule FavnOrchestrator.Persistence.TargetOperationLockStore do
  @moduledoc "Persistence authority for fenced multi-target write-operation leases."

  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.RenewTargetOperationLocks
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.TargetOperationLock

  @callback acquire_many(AcquireTargetOperationLocks.t()) ::
              {:ok, [TargetOperationLock.t()]} | {:error, Error.t()}
  @callback renew_many(RenewTargetOperationLocks.t()) ::
              {:ok, [TargetOperationLock.t()]} | {:error, Error.t()}
  @callback release_many(ReleaseTargetOperationLocks.t()) :: :ok | {:error, Error.t()}
end
