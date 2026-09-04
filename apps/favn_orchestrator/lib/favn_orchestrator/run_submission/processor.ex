defmodule FavnOrchestrator.RunSubmission.Processor do
  @moduledoc false

  alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted
  alias FavnOrchestrator.Persistence.Commands.RequeueRunSubmission
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunSubmission.Preparation
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.JsonSafe

  @default_max_attempts 5
  @default_retry_backoff_ms 1_000
  @maximum_retry_backoff_ms 30_000

  @type result ::
          {:ok, RunSubmission.t()}
          | {:retry, term()}
          | {:error, term()}

  @spec process(RunSubmission.t(), keyword()) :: result()
  def process(%RunSubmission{} = submission, opts) when is_list(opts) do
    context = SystemContext.workspace(submission.workspace_id, :run_submission_worker)

    cond do
      submission.status in [:preparing, :admitting] and
          not is_nil(submission.cancellation_requested_at) ->
        reconcile_cancellation(context, submission, opts)

      submission.status == :preparing ->
        prepare_and_admit(context, submission, opts)

      submission.status == :admitting ->
        reconcile_or_admit(context, submission, opts)

      true ->
        {:error, {:invalid_run_submission_worker_status, submission.status}}
    end
  rescue
    error -> fail_before_admission(submission, {:exception, error}, opts)
  catch
    kind, reason -> fail_before_admission(submission, {kind, reason}, opts)
  end

  defp reconcile_cancellation(context, submission, opts) do
    case durable_run(context, submission, opts) do
      {:ok, _run} ->
        manager = Keyword.get(opts, :run_manager, RunManager)

        case manager.cancel_run(context, submission.run_id, %{reason: :submission_cancelled}) do
          :ok -> mark_submitted(context, submission, opts)
          {:error, :run_already_terminal} -> mark_submitted(context, submission, opts)
          {:error, reason} -> {:retry, reason}
        end

      {:error, %Error{kind: :not_found}} ->
        acknowledge_cancellation(context, submission, opts)

      {:error, reason} ->
        {:retry, {:run_reconciliation_unavailable, redacted(reason)}}
    end
  end

  defp prepare_and_admit(context, submission, opts) do
    preparation = Keyword.get(opts, :preparation, Preparation)

    case preparation.prepare(context, submission) do
      {:ok, prepared, summary} ->
        with {:ok, admitting} <-
               store(opts).mark_admitting(%MarkRunSubmissionAdmitting{
                 workspace_context: context,
                 command_id: command_id("admitting", submission),
                 submission_id: submission.submission_id,
                 owner_id: submission.claim_owner,
                 claim_generation: submission.claim_generation,
                 preparation: summary,
                 occurred_at: now(opts)
               }) do
          admit(context, admitting, prepared, opts)
        end

      {:error, reason} ->
        fail_before_admission(submission, reason, opts)
    end
  end

  defp reconcile_or_admit(context, submission, opts) do
    case durable_run(context, submission, opts) do
      {:ok, _run} ->
        mark_submitted(context, submission, opts)

      {:error, %Error{kind: :not_found}} ->
        preparation = Keyword.get(opts, :preparation, Preparation)

        case preparation.prepare(context, submission) do
          {:ok, prepared, _summary} ->
            admit(context, submission, prepared, opts)

          {:error, reason} ->
            fail_admitting_after_reconciliation(context, submission, reason, opts)
        end

      {:error, reason} ->
        {:retry, {:run_reconciliation_unavailable, redacted(reason)}}
    end
  end

  defp admit(context, submission, prepared, opts) do
    run_manager = Keyword.get(opts, :run_manager, RunManager)

    case run_manager.admit_claimed_submission(prepared) do
      {:ok, run_id} when run_id == submission.run_id ->
        mark_submitted(context, submission, opts)

      {:ok, other_run_id} ->
        fail_admitting_after_reconciliation(
          context,
          submission,
          {:run_identity_mismatch, other_run_id},
          opts
        )

      {:error, reason} ->
        reconcile_admission_error(context, submission, reason, opts)
    end
  rescue
    error ->
      reconcile_admission_error(context, submission, {:exception, error}, opts)
  catch
    kind, reason ->
      reconcile_admission_error(context, submission, {kind, reason}, opts)
  end

  defp reconcile_admission_error(context, submission, reason, opts) do
    case durable_run(context, submission, opts) do
      {:ok, _run} ->
        mark_submitted(context, submission, opts)

      {:error, %Error{kind: :not_found}} ->
        fail_admitting_after_reconciliation(context, submission, reason, opts)

      {:error, reconciliation_error} ->
        {:retry, {:admission_outcome_unknown, redacted(reason), redacted(reconciliation_error)}}
    end
  end

  defp durable_run(context, submission, opts) do
    runs = Keyword.get(opts, :runs, Runs)
    runs.get(context, submission.run_id)
  rescue
    error -> {:error, {:run_reconciliation_exception, error}}
  catch
    kind, reason -> {:error, {:run_reconciliation_exception, {kind, reason}}}
  end

  defp mark_submitted(context, submission, opts) do
    store(opts).mark_submitted(%MarkRunSubmissionSubmitted{
      workspace_context: context,
      command_id: command_id("submitted", submission),
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      run_id: submission.run_id,
      outcome: %{"run_id" => submission.run_id},
      occurred_at: now(opts)
    })
  rescue
    error -> {:retry, {:run_submission_terminal_persistence_unknown, redacted(error)}}
  catch
    kind, reason ->
      {:retry, {:run_submission_terminal_persistence_unknown, redacted({kind, reason})}}
  end

  defp acknowledge_cancellation(context, submission, opts) do
    store(opts).acknowledge_cancellation(%AcknowledgeRunSubmissionCancellation{
      workspace_context: context,
      command_id: command_id("cancelled", submission),
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      occurred_at: now(opts)
    })
  end

  defp fail_before_admission(%RunSubmission{status: :preparing} = submission, reason, opts) do
    context = SystemContext.workspace(submission.workspace_id, :run_submission_worker)

    if retryable?(reason) do
      retry_or_fail(context, submission, reason, opts)
    else
      mark_failed(context, submission, :permanent, reason, opts)
    end
  end

  defp fail_before_admission(submission, reason, _opts),
    do: {:error, {:run_submission_failure_after_admission, submission.status, redacted(reason)}}

  defp fail_admitting_after_reconciliation(
         context,
         submission,
         %Error{details: %{reason: :run_cancelled}},
         opts
       ) do
    acknowledge_cancellation(context, submission, opts)
  end

  defp fail_admitting_after_reconciliation(context, submission, reason, opts) do
    cond do
      retryable?(reason) ->
        retry_or_fail(context, submission, reason, opts)

      permanent_admission_failure?(reason) ->
        mark_failed(context, submission, :permanent, reason, opts)

      true ->
        mark_failed(context, submission, :unknown, {:admission_outcome_unknown, reason}, opts)
    end
  end

  defp retry_or_fail(context, submission, reason, opts) do
    if submission.attempt >= Keyword.get(opts, :max_attempts, @default_max_attempts) do
      mark_failed(context, submission, :safe, {:retry_limit_reached, reason}, opts)
    else
      backoff_ms = retry_backoff_ms(submission.attempt, opts)
      occurred_at = now(opts)

      store(opts).requeue(%RequeueRunSubmission{
        workspace_context: context,
        command_id: command_id("requeue", submission),
        submission_id: submission.submission_id,
        owner_id: submission.claim_owner,
        claim_generation: submission.claim_generation,
        reason: %{"reason" => redacted(reason), "backoff_ms" => backoff_ms},
        available_at: DateTime.add(occurred_at, backoff_ms, :millisecond),
        occurred_at: occurred_at
      })
    end
  end

  defp mark_failed(context, submission, failure_kind, reason, opts) do
    store(opts).mark_failed(%MarkRunSubmissionFailed{
      workspace_context: context,
      command_id: command_id("failed", submission),
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      failure_kind: failure_kind,
      error: %{"reason" => redacted(reason)},
      occurred_at: now(opts)
    })
  end

  defp retryable?(%Error{retryable?: retryable?}), do: retryable?
  defp retryable?({:run_plan_capacity_exhausted, _details}), do: true
  defp retryable?(_reason), do: false

  defp permanent_admission_failure?(%Error{retryable?: false}), do: true

  defp permanent_admission_failure?({:run_plan_exceeds_node_capacity, _required, _maximum}),
    do: true

  defp permanent_admission_failure?(_reason), do: false

  defp retry_backoff_ms(attempt, opts) do
    base = Keyword.get(opts, :retry_backoff_ms, @default_retry_backoff_ms)
    multiplier = Integer.pow(2, max(attempt - 1, 0))
    min(base * multiplier, @maximum_retry_backoff_ms)
  end

  defp command_id(operation, submission) do
    identity =
      "#{submission.submission_id}:#{submission.claim_generation}:#{operation}"
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "run-submission:#{operation}:#{identity}"
  end

  defp redacted(reason), do: JsonSafe.error(reason)
  defp store(opts), do: Keyword.fetch!(opts, :store)
  defp now(opts), do: Keyword.get(opts, :now, DateTime.utc_now())
end
