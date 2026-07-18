defmodule FavnOrchestrator.Persistence.SchedulerStore do
  @moduledoc "Persistence contract for distributed schedule evaluation and occurrence dispatch."
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences
  alias FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation
  alias FavnOrchestrator.Persistence.Commands.CompleteScheduleOccurrence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.PageScheduleOccurrences
  alias FavnOrchestrator.Persistence.Queries.PageSchedules
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Schedule
  alias FavnOrchestrator.Persistence.Results.ScheduleClaim
  alias FavnOrchestrator.Persistence.Results.ScheduleOccurrence

  @callback claim_due_schedules(ClaimDueSchedules.t()) ::
              {:ok, [ScheduleClaim.t()]} | {:error, Error.t()}
  @callback commit_evaluation(CommitScheduleEvaluation.t()) ::
              {:ok, [ScheduleOccurrence.t()]} | {:error, Error.t()}
  @callback claim_occurrences(ClaimScheduleOccurrences.t()) ::
              {:ok, [ScheduleOccurrence.t()]} | {:error, Error.t()}
  @callback complete_occurrence(CompleteScheduleOccurrence.t()) ::
              {:ok, ScheduleOccurrence.t()} | {:error, Error.t()}
  @callback page_schedules(PageSchedules.t()) ::
              {:ok, CursorPage.t(Schedule.t())} | {:error, Error.t()}
  @callback page_occurrences(PageScheduleOccurrences.t()) ::
              {:ok, CursorPage.t(ScheduleOccurrence.t())} | {:error, Error.t()}
end
