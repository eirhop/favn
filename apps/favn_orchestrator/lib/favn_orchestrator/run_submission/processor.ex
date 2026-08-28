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
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.BoundedWorker
  alias FavnOrchestrator.MemoryCapacity.Budget
  alias FavnOrchestrator.MemoryCapacity.Error, as: MemoryError
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunManager.Submission
  alias FavnOrchestrator.RunSubmission.Preparation
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Storage.JsonSafe

  @default_max_attempts 5
  @default_retry_backoff_ms 1_000
  @maximum_retry_backoff_ms 30_000
  @run_plan_handoff_copies 3

  @type result ::
          {:ok, RunSubmission.t()}
          | {:retry, term()}
          | {:error, term()}

  @spec process(RunSubmission.t(), keyword()) :: result()
  def process(%RunSubmission{} = submission, opts) when is_list(opts) do
    context = SystemContext.workspace(submission.workspace_id, :run_submission_worker)

    cond do
      submission.status == :preparing and not is_nil(submission.cancellation_requested_at) ->
        acknowledge_cancellation(context, submission, opts)

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

  defp prepare_and_admit(context, submission, opts) do
    preparation = Keyword.get(opts, :preparation, Preparation)

    if preparation == Preparation and Keyword.get(opts, :run_manager, RunManager) == RunManager do
      prepare_and_admit_with_memory(context, submission, opts)
    else
      prepare_and_admit_unmanaged(context, submission, opts, preparation)
    end
  end

  defp prepare_and_admit_with_memory(context, submission, opts) do
    retained_budget = active_run_plan_max_bytes()
    construction_budget = Budget.worker_handoff(retained_budget)

    case MemoryCapacity.acquire(construction_budget, kind: :run_plan_build) do
      {:ok, token} ->
        result =
          BoundedWorker.run_serialized(
            fn -> Preparation.prepare(context, submission, memory_capacity_token: token) end,
            construction_budget,
            retained_budget
          )

        case result do
          {:ok, %Submission{} = prepared, summary} ->
            retain_and_admit(context, submission, prepared, summary, token, opts)

          {:error, reason} ->
            MemoryCapacity.release(token)
            fail_before_admission(submission, reason, opts)

          other ->
            MemoryCapacity.release(token)
            fail_before_admission(submission, {:invalid_run_preparation_result, other}, opts)
        end

      {:error, %MemoryError{} = error} ->
        fail_before_admission(submission, error, opts)
    end
  end

  defp retain_and_admit(context, submission, prepared, summary, token, opts) do
    retained_bytes = PlanCapacity.retained_term_bytes(prepared)

    with :ok <- MemoryCapacity.transfer(token, retained_bytes),
         prepared = %{
           prepared
           | memory_capacity_token: token,
             memory_capacity_bytes: retained_bytes
         },
         {:ok, admitting} <- mark_admitting(context, submission, summary, opts) do
      admit_with_memory_handoff(context, admitting, prepared, token, retained_bytes, opts)
    else
      {:error, reason} ->
        MemoryCapacity.release(token)
        fail_before_admission(submission, reason, opts)
    end
  end

  defp prepare_and_admit_unmanaged(context, submission, opts, preparation) do
    case preparation.prepare(context, submission) do
      {:ok, prepared, summary} ->
        with {:ok, admitting} <- mark_admitting(context, submission, summary, opts) do
          admit(context, admitting, prepared, opts)
        end

      {:error, reason} ->
        fail_before_admission(submission, reason, opts)
    end
  end

  defp mark_admitting(context, submission, summary, opts) do
    store(opts).mark_admitting(%MarkRunSubmissionAdmitting{
      workspace_context: context,
      command_id: command_id("admitting", submission),
      submission_id: submission.submission_id,
      owner_id: submission.claim_owner,
      claim_generation: submission.claim_generation,
      preparation: summary,
      occurred_at: now(opts)
    })
  end

  defp reconcile_or_admit(context, submission, opts) do
    case durable_run(context, submission, opts) do
      {:ok, _run} ->
        mark_submitted(context, submission, opts)

      {:error, %Error{kind: :not_found}} ->
        preparation = Keyword.get(opts, :preparation, Preparation)

        if preparation == Preparation and
             Keyword.get(opts, :run_manager, RunManager) == RunManager do
          reconcile_and_admit_with_memory(context, submission, opts)
        else
          case preparation.prepare(context, submission) do
            {:ok, prepared, _summary} ->
              admit(context, submission, prepared, opts)

            {:error, reason} ->
              fail_admitting_after_reconciliation(context, submission, reason, opts)
          end
        end

      {:error, reason} ->
        {:retry, {:run_reconciliation_unavailable, redacted(reason)}}
    end
  end

  defp reconcile_and_admit_with_memory(context, submission, opts) do
    retained_budget = active_run_plan_max_bytes()
    construction_budget = Budget.worker_handoff(retained_budget)

    case MemoryCapacity.acquire(construction_budget, kind: :run_plan_build) do
      {:ok, token} ->
        result =
          BoundedWorker.run_serialized(
            fn -> Preparation.prepare(context, submission, memory_capacity_token: token) end,
            construction_budget,
            retained_budget
          )

        case result do
          {:ok, %Submission{} = prepared, _summary} ->
            retained_bytes = PlanCapacity.retained_term_bytes(prepared)

            case MemoryCapacity.transfer(token, retained_bytes) do
              :ok ->
                admit_with_memory_handoff(
                  context,
                  submission,
                  %{
                    prepared
                    | memory_capacity_token: token,
                      memory_capacity_bytes: retained_bytes
                  },
                  token,
                  retained_bytes,
                  opts
                )

              {:error, reason} ->
                MemoryCapacity.release(token)
                fail_admitting_after_reconciliation(context, submission, reason, opts)
            end

          {:error, reason} ->
            MemoryCapacity.release(token)
            fail_admitting_after_reconciliation(context, submission, reason, opts)

          other ->
            MemoryCapacity.release(token)

            fail_admitting_after_reconciliation(
              context,
              submission,
              {:invalid_run_preparation_result, other},
              opts
            )
        end

      {:error, %MemoryError{} = error} ->
        fail_admitting_after_reconciliation(context, submission, error, opts)
    end
  end

  defp admit(context, submission, prepared, opts, handoff_token \\ nil) do
    run_manager = Keyword.get(opts, :run_manager, RunManager)

    result =
      case handoff_token do
        %MemoryCapacity{} = token -> run_manager.admit_claimed_submission(prepared, token)
        nil -> run_manager.admit_claimed_submission(prepared)
      end

    case result do
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
      release_optional_token(handoff_token)
      reconcile_admission_error(context, submission, {:exception, error}, opts)
  catch
    kind, reason ->
      release_optional_token(handoff_token)
      reconcile_admission_error(context, submission, {kind, reason}, opts)
  end

  defp admit_with_memory_handoff(
         context,
         submission,
         prepared,
         retained_token,
         retained_bytes,
         opts
       ) do
    handoff_bytes = @run_plan_handoff_copies * retained_bytes

    case MemoryCapacity.acquire(handoff_bytes, kind: :run_plan_handoff) do
      {:ok, handoff_token} ->
        admit(context, submission, prepared, opts, handoff_token)

      {:error, %MemoryError{} = error} ->
        MemoryCapacity.release(retained_token)
        fail_admitting_after_reconciliation(context, submission, error, opts)
    end
  end

  defp release_optional_token(%MemoryCapacity{} = token), do: MemoryCapacity.release(token)
  defp release_optional_token(nil), do: :ok

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
    if not capacity_retry?(reason) and
         submission.attempt >= Keyword.get(opts, :max_attempts, @default_max_attempts) do
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
  defp retryable?(%MemoryError{}), do: true
  defp retryable?({:run_plan_capacity_exhausted, _details}), do: true
  defp retryable?(reason) when reason in [:worker_failed, :worker_timeout], do: true
  defp retryable?(_reason), do: false

  defp capacity_retry?(%MemoryError{}), do: true
  defp capacity_retry?({:run_plan_capacity_exhausted, _details}), do: true
  defp capacity_retry?(reason) when reason in [:worker_failed, :worker_timeout], do: true
  defp capacity_retry?(_reason), do: false

  defp permanent_admission_failure?(%Error{retryable?: false}), do: true

  defp permanent_admission_failure?({:run_plan_exceeds_node_capacity, _required, _maximum}),
    do: true

  defp permanent_admission_failure?(_reason), do: false

  defp retry_backoff_ms(attempt, opts) do
    base = Keyword.get(opts, :retry_backoff_ms, @default_retry_backoff_ms)
    multiplier = Integer.pow(2, attempt |> Kernel.-(1) |> max(0) |> min(5))
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

  defp active_run_plan_max_bytes do
    Application.get_env(:favn_orchestrator, :active_run_plan_max_bytes, 512 * 1_024 * 1_024)
  end
end
