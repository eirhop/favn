defmodule FavnStoragePostgres.RunSubmissions.Codec do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.RunSubmissionAuthority
  alias FavnStoragePostgres.Schemas.RunSubmission, as: RunSubmissionSchema

  @spec result(struct()) :: RunSubmission.t()
  def result(%RunSubmissionSchema{} = submission) do
    %RunSubmission{
      workspace_id: submission.workspace_id,
      submission_id: submission.submission_id,
      source: existing_atom!(submission.source),
      idempotency_key: submission.idempotency_key,
      request_hash: submission.request_hash,
      authority: RunSubmissionAuthority.load!(submission.authority),
      deployment_id: submission.deployment_id,
      manifest_version_id: submission.manifest_version_id,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      run_id: submission.run_id,
      intent: submission.intent,
      status: existing_atom!(submission.status),
      attempt: submission.attempt,
      claim_owner: submission.claim_owner,
      claim_generation: submission.claim_generation,
      claim_expires_at: submission.claim_expires_at,
      preparation: submission.preparation,
      outcome: submission.outcome,
      error: submission.error,
      failure_kind: optional_existing_atom(submission.failure_kind),
      cancellation_requested_at: submission.cancellation_requested_at,
      cancellation_reason: submission.cancellation_reason,
      retry_root_id: submission.retry_root_id,
      retry_of_submission_id: submission.retry_of_submission_id,
      retry_command_id: submission.retry_command_id,
      superseded_by_submission_id: submission.superseded_by_submission_id,
      enqueued_at: submission.enqueued_at,
      available_at: submission.available_at,
      preparing_at: submission.preparing_at,
      admitting_at: submission.admitting_at,
      terminal_at: submission.terminal_at,
      inserted_at: submission.inserted_at,
      updated_at: submission.updated_at
    }
  end

  defp optional_existing_atom(nil), do: nil
  defp optional_existing_atom(value), do: existing_atom!(value)

  defp existing_atom!(value) when is_binary(value), do: String.to_existing_atom(value)
end
