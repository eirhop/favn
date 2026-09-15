defmodule FavnOrchestrator.Persistence.MaintenanceStore do
  @moduledoc "Persistence contract for explicit, bounded backfill, repair, and retention."

  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.ReconcilePersistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.MaintenanceOutcome

  @callback backfill_missing_projection(BackfillMissingProjection.t()) ::
              {:ok, MaintenanceOutcome.t()} | {:error, Error.t()}
  @callback reconcile(ReconcilePersistence.t()) ::
              {:ok, MaintenanceOutcome.t()} | {:error, Error.t()}
  @callback retention_status(FavnOrchestrator.Persistence.PlatformContext.t()) ::
              {:ok, map()} | {:error, Error.t()}
  @callback retention_preview(
              FavnOrchestrator.Persistence.PlatformContext.t(),
              FavnOrchestrator.Retention.Policy.family()
            ) :: {:ok, map()} | {:error, Error.t()}
  @callback retention_batch(FavnOrchestrator.Persistence.Commands.RetentionBatch.t()) ::
              {:ok, map()} | {:error, Error.t()}
  @callback configure_retention(FavnOrchestrator.Persistence.Commands.ConfigureRetention.t()) ::
              {:ok, map()} | {:error, Error.t()}
end
