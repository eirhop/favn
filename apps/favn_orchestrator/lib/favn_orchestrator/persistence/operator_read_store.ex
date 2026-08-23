defmodule FavnOrchestrator.Persistence.OperatorReadStore do
  @moduledoc "Persistence contract for bounded operator and customer read models."

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt
  alias FavnOrchestrator.Persistence.Queries.GetRunFlow
  alias FavnOrchestrator.Persistence.Queries.GetRunHeader
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.CountSuccessfulAssetWindows
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.ListRunEventSummaries
  alias FavnOrchestrator.Persistence.Queries.ListRunWindows
  alias FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys
  alias FavnOrchestrator.Persistence.Queries.CountExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageGroupRuns
  alias FavnOrchestrator.Persistence.Queries.PageGroupWindows
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.ExecutionGroup
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupCounts
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.FreshnessState
  alias FavnOrchestrator.Persistence.Results.ManifestSummary
  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt
  alias FavnOrchestrator.Persistence.Results.RunEventSummary
  alias FavnOrchestrator.Persistence.Results.RunFlowSnapshot
  alias FavnOrchestrator.Persistence.Results.RunWindowChoices
  alias FavnOrchestrator.Persistence.Results.RunViewHeader
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.Results.TargetStatus

  @callback page_manifests(PageManifests.t()) ::
              {:ok, CursorPage.t(ManifestSummary.t())} | {:error, Error.t()}
  @callback page_execution_groups(PageExecutionGroups.t()) ::
              {:ok, CursorPage.t(ExecutionGroupOverview.t())} | {:error, Error.t()}
  @callback count_execution_groups(CountExecutionGroups.t()) ::
              {:ok, ExecutionGroupCounts.t()} | {:error, Error.t()}
  @callback get_execution_group(GetExecutionGroup.t()) ::
              {:ok, ExecutionGroup.t()} | {:error, Error.t()}
  @callback get_run_flow(GetRunFlow.t()) ::
              {:ok, RunFlowSnapshot.t()} | {:error, Error.t()}
  @callback get_run_header(GetRunHeader.t()) ::
              {:ok, RunViewHeader.t()} | {:error, Error.t()}
  @callback list_run_windows(ListRunWindows.t()) ::
              {:ok, RunWindowChoices.t()} | {:error, Error.t()}
  @callback get_run_asset_attempt(GetRunAssetAttempt.t()) ::
              {:ok, RunAssetAttempt.t()} | {:error, Error.t()}
  @callback list_run_event_summaries(ListRunEventSummaries.t()) ::
              {:ok, [RunEventSummary.t()]} | {:error, Error.t()}
  @callback page_group_runs(PageGroupRuns.t()) ::
              {:ok, CursorPage.t(RunSummary.t())} | {:error, Error.t()}
  @callback page_group_windows(PageGroupWindows.t()) ::
              {:ok, CursorPage.t(BackfillWindow.t())} | {:error, Error.t()}
  @callback get_target_statuses(GetTargetStatuses.t()) ::
              {:ok, [TargetStatus.t()]} | {:error, Error.t()}
  @callback page_target_runs(PageTargetRuns.t()) ::
              {:ok, CursorPage.t(RunSummary.t())} | {:error, Error.t()}
  @callback get_freshness_many(GetFreshnessMany.t()) ::
              {:ok, [FreshnessState.t()]} | {:error, Error.t()}
  @callback get_asset_window_states(GetAssetWindowStates.t()) ::
              {:ok, [FavnOrchestrator.Persistence.Results.AssetWindowState.t()]}
              | {:error, Error.t()}
  @callback count_successful_asset_windows(CountSuccessfulAssetWindows.t()) ::
              {:ok, non_neg_integer()} | {:error, Error.t()}
  @callback get_successful_asset_window_keys(GetSuccessfulAssetWindowKeys.t()) ::
              {:ok, [String.t()]} | {:error, Error.t()}
end
