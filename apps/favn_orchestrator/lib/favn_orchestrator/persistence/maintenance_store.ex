defmodule FavnOrchestrator.Persistence.MaintenanceStore do
  @moduledoc "Persistence contract for explicit, bounded backfill, repair, and retention."

  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.PurgePersistence
  alias FavnOrchestrator.Persistence.Commands.ReconcilePersistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.MaintenanceOutcome

  @callback backfill_missing_projection(BackfillMissingProjection.t()) ::
              {:ok, MaintenanceOutcome.t()} | {:error, Error.t()}
  @callback reconcile(ReconcilePersistence.t()) ::
              {:ok, MaintenanceOutcome.t()} | {:error, Error.t()}
  @callback purge(PurgePersistence.t()) ::
              {:ok, MaintenanceOutcome.t()} | {:error, Error.t()}
end
