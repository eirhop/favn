defmodule FavnOrchestrator.RunSubmission.ProcessorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunSubmissionCancellation
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionAdmitting
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionFailed
  alias FavnOrchestrator.Persistence.Commands.MarkRunSubmissionSubmitted
  alias FavnOrchestrator.Persistence.Commands.RequeueRunSubmission
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.RunSubmissionAuthority
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.MemoryCapacity.Error, as: MemoryError
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunSubmission.Processor

  defmodule Store do
    def mark_admitting(command) do
      notify(command)
      {:ok, %{submission() | status: :admitting, preparation: command.preparation}}
    end

    def mark_submitted(command) do
      notify(command)
      {:ok, %{submission() | status: :submitted, outcome: command.outcome}}
    end

    def mark_failed(command) do
      notify(command)

      {:ok,
       %{
         submission()
         | status: :failed,
           failure_kind: command.failure_kind,
           error: command.error
       }}
    end

    def requeue(command) do
      notify(command)
      {:ok, %{submission() | status: :queued, available_at: command.available_at}}
    end

    def acknowledge_cancellation(command) do
      notify(command)
      {:ok, %{submission() | status: :cancelled}}
    end

    defp notify(command), do: send(test_process(), {:store_command, command})
    defp submission, do: :persistent_term.get({__MODULE__, :submission})
    defp test_process, do: :persistent_term.get({__MODULE__, :test})
  end

  defmodule Preparation do
    def prepare(_context, _submission), do: :persistent_term.get({__MODULE__, :result})
  end

  defmodule RunManager do
    def admit_claimed_submission(_prepared),
      do: :persistent_term.get({__MODULE__, :result})
  end

  defmodule RaisingRunManager do
    def admit_claimed_submission(_prepared), do: raise("admission transport crashed")
  end

  defmodule Runs do
    def get(_context, _run_id), do: :persistent_term.get({__MODULE__, :result})
  end

  setup do
    submission = submission()
    :persistent_term.put({Store, :submission}, submission)
    :persistent_term.put({Store, :test}, self())
    :persistent_term.put({Preparation, :result}, {:ok, :prepared, %{"plan" => "bounded"}})
    :persistent_term.put({RunManager, :result}, {:ok, submission.run_id})
    :persistent_term.put({Runs, :result}, {:error, Error.new(:not_found, "not found")})

    on_exit(fn ->
      for key <- [
            {Store, :submission},
            {Store, :test},
            {Preparation, :result},
            {RunManager, :result},
            {Runs, :result}
          ],
          do: :persistent_term.erase(key)
    end)

    {:ok, submission: submission}
  end

  test "requeues retryable preparation failures before admission", %{submission: submission} do
    :persistent_term.put(
      {Preparation, :result},
      {:error, Error.new(:unavailable, "temporarily unavailable", retryable?: true)}
    )

    assert {:ok, %{status: :queued}} =
             Processor.process(submission, processor_options())

    assert_receive {:store_command, %RequeueRunSubmission{} = command}
    assert command.submission_id == submission.submission_id
    assert DateTime.compare(command.available_at, command.occurred_at) == :gt
  end

  test "stops bounded retry at the configured attempt limit", %{submission: submission} do
    :persistent_term.put(
      {Preparation, :result},
      {:error, Error.new(:unavailable, "temporarily unavailable", retryable?: true)}
    )

    exhausted = %{submission | attempt: 3}
    :persistent_term.put({Store, :submission}, exhausted)

    assert {:ok, %{status: :failed, failure_kind: :safe}} =
             Processor.process(exhausted, processor_options())

    assert_receive {:store_command, %MarkRunSubmissionFailed{failure_kind: :safe}}
    refute_receive {:store_command, %RequeueRunSubmission{}}
  end

  test "memory pressure keeps deferring after the ordinary retry limit", %{submission: submission} do
    :persistent_term.put(
      {Preparation, :result},
      {:error, %MemoryError{code: :manifest_capacity_unavailable, required_bytes: 1}}
    )

    exhausted = %{submission | attempt: 100}
    :persistent_term.put({Store, :submission}, exhausted)

    assert {:ok, %{status: :queued}} =
             Processor.process(exhausted, processor_options())

    assert_receive {:store_command, %RequeueRunSubmission{}}
    refute_receive {:store_command, %MarkRunSubmissionFailed{}}
  end

  test "bounded worker availability keeps deferring after the ordinary retry limit", %{
    submission: submission
  } do
    :persistent_term.put({Preparation, :result}, {:error, :worker_timeout})

    exhausted = %{submission | attempt: 100}
    :persistent_term.put({Store, :submission}, exhausted)

    assert {:ok, %{status: :queued}} =
             Processor.process(exhausted, processor_options())

    assert_receive {:store_command, %RequeueRunSubmission{}}
    refute_receive {:store_command, %MarkRunSubmissionFailed{}}
  end

  test "marks an unreconciled ambiguous admission failure unknown", %{submission: submission} do
    admitting = %{submission | status: :admitting}
    :persistent_term.put({Store, :submission}, admitting)
    :persistent_term.put({RunManager, :result}, {:error, :transport_closed})

    assert {:ok, %{status: :failed, failure_kind: :unknown}} =
             Processor.process(admitting, processor_options())

    assert_receive {:store_command, %MarkRunSubmissionFailed{failure_kind: :unknown} = command}

    assert command.submission_id == submission.submission_id
  end

  test "contains admission exceptions and preserves the outcome as unknown", %{
    submission: submission
  } do
    assert {:ok, %{status: :failed, failure_kind: :unknown}} =
             Processor.process(
               submission,
               Keyword.put(processor_options(), :run_manager, RaisingRunManager)
             )

    assert_receive {:store_command, %MarkRunSubmissionAdmitting{}}

    assert_receive {:store_command, %MarkRunSubmissionFailed{failure_kind: :unknown} = command}

    assert command.submission_id == submission.submission_id
  end

  test "recognizes an already durable run after restart without admitting again", %{
    submission: submission
  } do
    admitting = %{submission | status: :admitting}
    :persistent_term.put({Store, :submission}, admitting)
    :persistent_term.put({Runs, :result}, {:ok, %RunState{id: submission.run_id}})

    assert {:ok, %{status: :submitted}} =
             Processor.process(admitting, processor_options())

    assert_receive {:store_command, %MarkRunSubmissionSubmitted{} = command}
    assert command.run_id == submission.run_id
  end

  test "acknowledges cancellation before planning or admission", %{submission: submission} do
    cancelling = %{submission | cancellation_requested_at: DateTime.utc_now()}
    :persistent_term.put({Store, :submission}, cancelling)

    assert {:ok, %{status: :cancelled}} =
             Processor.process(cancelling, processor_options())

    assert_receive {:store_command, %AcknowledgeRunSubmissionCancellation{}}
    refute_receive {:store_command, %MarkRunSubmissionAdmitting{}}
  end

  defp processor_options do
    [
      store: Store,
      preparation: Preparation,
      run_manager: RunManager,
      runs: Runs,
      retry_backoff_ms: 10,
      max_attempts: 3,
      now: ~U[2026-07-26 12:00:00Z]
    ]
  end

  defp submission do
    now = ~U[2026-07-26 11:59:00Z]

    %RunSubmission{
      workspace_id: "workspace-a",
      submission_id: "submission-a",
      source: :operator,
      idempotency_key: "idempotency-a",
      request_hash: :crypto.hash(:sha256, "request"),
      authority: %RunSubmissionAuthority{
        workspace_id: "workspace-a",
        principal_id: "operator-a",
        roles: [:customer_operator]
      },
      deployment_id: "deployment-a",
      manifest_version_id: "manifest-a",
      target_kind: "asset",
      target_id: "asset-a",
      run_id: "run-a",
      intent: %{"format" => "test"},
      status: :preparing,
      attempt: 1,
      claim_owner: "worker-a",
      claim_generation: 1,
      claim_expires_at: DateTime.add(now, 60_000, :millisecond),
      retry_root_id: "submission-a",
      enqueued_at: now,
      available_at: now,
      inserted_at: now,
      updated_at: now
    }
  end
end
