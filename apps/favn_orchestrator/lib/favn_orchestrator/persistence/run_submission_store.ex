defmodule FavnOrchestrator.Persistence.RunSubmissionStore do
  @moduledoc "Persistence contract for durable run-submission intent and fenced preparation."

  alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.ClaimRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.ClaimStaleRunSubmissions
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted
  alias FavnOrchestrator.Persistence.Commands.RenewRunSubmissionClaim
  alias FavnOrchestrator.Persistence.Commands.RequestRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.RequeueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.RetryFailedRunSubmission
  alias FavnOrchestrator.Persistence.Commands.SupersedeRunSubmission
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRunSubmission
  alias FavnOrchestrator.Persistence.Queries.PageRunSubmissions
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.Results.RunSubmissionPage

  @callback enqueue(EnqueueRunSubmission.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback claim(ClaimRunSubmissions.t()) ::
              {:ok, [RunSubmission.t()]} | {:error, Error.t()}
  @callback claim_stale(ClaimStaleRunSubmissions.t()) ::
              {:ok, [RunSubmission.t()]} | {:error, Error.t()}
  @callback renew(RenewRunSubmissionClaim.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback mark_admitting(MarkRunSubmissionAdmitting.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback mark_submitted(MarkRunSubmissionSubmitted.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback mark_failed(MarkRunSubmissionFailed.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback requeue(RequeueRunSubmission.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback request_cancellation(RequestRunSubmissionCancellation.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback acknowledge_cancellation(AcknowledgeRunSubmissionCancellation.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback supersede(SupersedeRunSubmission.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback retry_failed(RetryFailedRunSubmission.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback get(GetRunSubmission.t()) ::
              {:ok, RunSubmission.t()} | {:error, Error.t()}
  @callback page(PageRunSubmissions.t()) ::
              {:ok, RunSubmissionPage.t()} | {:error, Error.t()}
end
