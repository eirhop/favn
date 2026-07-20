defmodule FavnOrchestrator.Persistence.OperatorReadStore do
  @moduledoc "Persistence contract for bounded operator and customer read models."

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetOperatorRunOverview
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageGroupRuns
  alias FavnOrchestrator.Persistence.Queries.PageGroupWindows
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.ExecutionGroup
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.FreshnessState
  alias FavnOrchestrator.Persistence.Results.ManifestSummary
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverview
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.Results.TargetStatus

  @callback page_manifests(PageManifests.t()) ::
              {:ok, CursorPage.t(ManifestSummary.t())} | {:error, Error.t()}
  @callback page_execution_groups(PageExecutionGroups.t()) ::
              {:ok, CursorPage.t(ExecutionGroupOverview.t())} | {:error, Error.t()}
  @callback get_execution_group(GetExecutionGroup.t()) ::
              {:ok, ExecutionGroup.t()} | {:error, Error.t()}
  @callback get_operator_run_overview(GetOperatorRunOverview.t()) ::
              {:ok, OperatorRunOverview.t()} | {:error, Error.t()}
  @callback page_group_runs(PageGroupRuns.t()) ::
              {:ok, CursorPage.t(RunSummary.t())} | {:error, Error.t()}
  @callback page_group_windows(PageGroupWindows.t()) ::
              {:ok, CursorPage.t()} | {:error, Error.t()}
  @callback get_target_statuses(GetTargetStatuses.t()) ::
              {:ok, [TargetStatus.t()]} | {:error, Error.t()}
  @callback page_target_runs(PageTargetRuns.t()) ::
              {:ok, CursorPage.t(RunSummary.t())} | {:error, Error.t()}
  @callback get_freshness_many(GetFreshnessMany.t()) ::
              {:ok, [FreshnessState.t()]} | {:error, Error.t()}
  @callback get_asset_window_states(GetAssetWindowStates.t()) ::
              {:ok, [FavnOrchestrator.Persistence.Results.AssetWindowState.t()]}
              | {:error, Error.t()}
end
