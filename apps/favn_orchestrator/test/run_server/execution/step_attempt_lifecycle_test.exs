defmodule FavnOrchestrator.RunServer.Execution.StepAttemptLifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle
  alias FavnOrchestrator.RunState

  test "maps runner statuses to run statuses and event types" do
    assert StepAttemptLifecycle.map_runner_status(:ok) == :ok
    assert StepAttemptLifecycle.map_runner_status(:cancelled) == :cancelled
    assert StepAttemptLifecycle.map_runner_status(:timed_out) == :timed_out
    assert StepAttemptLifecycle.map_runner_status(:anything_else) == :error

    assert StepAttemptLifecycle.step_outcome(:ok) == {:step_finished, false}
    assert StepAttemptLifecycle.step_outcome(:timed_out) == {:step_timed_out, true}
    assert StepAttemptLifecycle.step_outcome(:error) == {:step_failed, true}
  end

  test "runner retryability respects structured non-retryable errors" do
    retryable = %RunnerResult{status: :error, error: :temporary, asset_results: []}

    non_retryable = %RunnerResult{
      status: :error,
      error: RunnerError.normalize(:bad_config, type: :missing_runtime_config, retryable?: false),
      asset_results: []
    }

    assert StepAttemptLifecycle.runner_result_retryable?(retryable)
    refute StepAttemptLifecycle.runner_result_retryable?(non_retryable)
  end

  test "schedule_retry returns explicit retry data until max attempts" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 2, retry_backoff_ms: 25),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      stage: 1,
      attempt: 1,
      max_attempts: 2,
      execution_pool: "default"
    }

    assert {:ok, retry} = StepAttemptLifecycle.schedule_retry(lifecycle, true)
    assert retry.next_attempt == 2
    assert retry.retry_after_ms == 25

    assert StepAttemptLifecycle.retry_event_payload(retry).asset_step_id == "step_lifecycle"
  end

  test "schedule_retry is terminal at max attempts or when not retryable" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 1),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      attempt: 1,
      max_attempts: 1
    }

    assert StepAttemptLifecycle.schedule_retry(lifecycle, true) == :terminal
    assert StepAttemptLifecycle.schedule_retry(lifecycle, false) == :terminal
  end

  defp run_state(opts) do
    RunState.new(
      id: "run_lifecycle_test",
      manifest_version_id: "mv_lifecycle_test",
      manifest_content_hash: "hash_lifecycle_test",
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 0)
    )
  end
end
