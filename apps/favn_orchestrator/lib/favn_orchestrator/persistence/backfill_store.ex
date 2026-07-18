defmodule FavnOrchestrator.Persistence.BackfillStore do
  @moduledoc "Persistence contract for resumable, fenced, bounded backfill plans."

  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.PageAssetWindows
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Results.Backfill
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.CursorPage

  @callback start_plan(StartBackfillPlan.t()) :: {:ok, Backfill.t()} | {:error, Error.t()}
  @callback append_plan_batch(AppendBackfillPlanBatch.t()) ::
              {:ok, Backfill.t()} | {:error, Error.t()}
  @callback activate_plan(ActivateBackfillPlan.t()) ::
              {:ok, Backfill.t()} | {:error, Error.t()}
  @callback claim_windows(ClaimBackfillWindows.t()) ::
              {:ok, [BackfillWindow.t()]} | {:error, Error.t()}
  @callback transition_window(TransitionBackfillWindow.t()) ::
              {:ok, BackfillWindow.t()} | {:error, Error.t()}
  @callback get_backfill(GetBackfill.t()) :: {:ok, Backfill.t()} | {:error, Error.t()}
  @callback page_windows(PageBackfillWindows.t()) ::
              {:ok, CursorPage.t(BackfillWindow.t())} | {:error, Error.t()}
  @callback page_asset_windows(PageAssetWindows.t()) ::
              {:ok, CursorPage.t(BackfillWindow.t())} | {:error, Error.t()}
end
