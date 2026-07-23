defmodule FavnOrchestrator.Persistence.RebuildStore do
  @moduledoc "Persistence authority for immutable rebuild plans and resumable saga checkpoints."

  alias FavnOrchestrator.Persistence.Commands.ActivateRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildItems
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.CreateRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildReconciliation
  alias FavnOrchestrator.Persistence.Commands.RetryRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.StartRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildAction
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildItem
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRebuild
  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems
  alias FavnOrchestrator.Persistence.Queries.PageRebuildOperations
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RebuildAction
  alias FavnOrchestrator.Persistence.Results.RebuildItem
  alias FavnOrchestrator.Persistence.Results.RebuildOperation

  @callback create_plan(CreateRebuildPlan.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback start_operation(StartRebuildOperation.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback request_cancellation(RequestRebuildCancellation.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback request_reconciliation(RequestRebuildReconciliation.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback retry_operation(RetryRebuildOperation.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback claim_operation(ClaimRebuildOperation.t()) ::
              {:ok, RebuildOperation.t() | nil} | {:error, Error.t()}
  @callback transition_operation(TransitionRebuildOperation.t()) ::
              {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback claim_items(ClaimRebuildItems.t()) ::
              {:ok, [RebuildItem.t()]} | {:error, Error.t()}
  @callback transition_item(TransitionRebuildItem.t()) ::
              {:ok, RebuildItem.t()} | {:error, Error.t()}
  @callback transition_action(TransitionRebuildAction.t()) ::
              {:ok, RebuildAction.t()} | {:error, Error.t()}
  @callback activate_generation(ActivateRebuildGeneration.t()) ::
              {:ok, RebuildAction.t()} | {:error, Error.t()}
  @callback transition_generation(TransitionRebuildGeneration.t()) ::
              :ok | {:error, Error.t()}
  @callback get(GetRebuild.t()) :: {:ok, RebuildOperation.t()} | {:error, Error.t()}
  @callback page_items(PageRebuildItems.t()) ::
              {:ok, CursorPage.t(RebuildItem.t())} | {:error, Error.t()}
  @callback page_operations(PageRebuildOperations.t()) ::
              {:ok, CursorPage.t(RebuildOperation.t())} | {:error, Error.t()}
end
