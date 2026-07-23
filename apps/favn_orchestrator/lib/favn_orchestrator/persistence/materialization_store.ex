defmodule FavnOrchestrator.Persistence.MaterializationStore do
  @moduledoc "Persistence contract for fenced materialization and immutable success facts."

  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Commands.RenewMaterializationClaim
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetMaterializations
  alias FavnOrchestrator.Persistence.Queries.GetRebuildMaterialization
  alias FavnOrchestrator.Persistence.Results.MaterializationClaim
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision

  @callback claim(ClaimMaterialization.t()) ::
              {:ok, MaterializationDecision.t()} | {:error, Error.t()}
  @callback renew(RenewMaterializationClaim.t()) ::
              {:ok, MaterializationClaim.t()} | {:error, Error.t()}
  @callback finish(FinishMaterialization.t()) ::
              {:ok, MaterializationDecision.t()} | {:error, Error.t()}
  @callback get_many(GetMaterializations.t()) ::
              {:ok, [MaterializationDecision.t()]} | {:error, Error.t()}
  @callback get_rebuild(GetRebuildMaterialization.t()) ::
              {:ok, MaterializationDecision.t()} | {:error, Error.t()}
end
