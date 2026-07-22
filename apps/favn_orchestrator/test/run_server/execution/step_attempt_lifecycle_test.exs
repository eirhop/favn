defmodule FavnOrchestrator.RunServer.Execution.StepAttemptLifecycleTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Version
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
    retryable = %RunnerResult{
      status: :error,
      error:
        RunnerError.normalize(:temporary,
          retryable?: true,
          outcome: :safe_failure
        ),
      asset_results: []
    }

    non_retryable = %RunnerResult{
      status: :error,
      error: RunnerError.normalize(:bad_config, type: :missing_runtime_config, retryable?: false),
      asset_results: []
    }

    assert StepAttemptLifecycle.runner_result_retryable?(retryable)
    refute StepAttemptLifecycle.runner_result_retryable?(non_retryable)

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: %{details: %{asset_retryable?: true}},
             asset_results: []
           })

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: RunnerError.normalize(%{details: %{asset_retryable?: true}}),
             asset_results: []
           })

    assert StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             error: %{details: %{asset_retryable?: true}, outcome: :safe_failure},
             asset_results: []
           })

    refute StepAttemptLifecycle.runner_result_retryable?(:malformed_result)

    refute StepAttemptLifecycle.runner_result_retryable?(%RunnerResult{
             status: :error,
             asset_results: [:malformed_asset_result]
           })
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

    assert {:ok, retry} = StepAttemptLifecycle.schedule_retry(lifecycle)
    assert retry.next_attempt == 2
    assert retry.retry_after_ms == 25

    assert StepAttemptLifecycle.retry_event_payload(retry).asset_step_id == "step_lifecycle"
  end

  test "retry-after raises the policy delay without adding attempts" do
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

    failure = RunnerError.new(retryable?: true, outcome: :safe_failure, retry_after_ms: 250)

    assert {:ok, retry} = StepAttemptLifecycle.schedule_retry(lifecycle, failure)
    assert retry.next_attempt == 2
    assert retry.retry_after_ms == 250
  end

  test "schedule_retry is terminal at max attempts" do
    lifecycle = %StepAttemptLifecycle{
      run: run_state(max_attempts: 1),
      node_key: {{MyApp.Assets.Lifecycle, :asset}, nil},
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      asset_step_id: "step_lifecycle",
      attempt: 1,
      max_attempts: 1
    }

    assert StepAttemptLifecycle.schedule_retry(lifecycle) == :terminal
  end

  test "attaches one absolute deadline to work before runner preparation" do
    run = run_state(max_attempts: 1) |> Map.put(:timeout_ms, 100)
    work = %Favn.Contracts.RunnerWork{run_id: run.id, metadata: %{}}
    before_attach = DateTime.utc_now()

    prepared = StepAttemptLifecycle.attach_deadline(work, run)
    deadline_at = StepAttemptLifecycle.deadline_at(prepared)

    assert DateTime.compare(deadline_at, before_attach) == :gt
    assert DateTime.diff(deadline_at, before_attach, :millisecond) <= 100
    assert StepAttemptLifecycle.deadline_at(prepared) == deadline_at
  end

  test "runner work preserves the logical run start across attempts" do
    run = run_state(max_attempts: 2)
    node_key = {{MyApp.Assets.Lifecycle, :asset}, nil}

    assert {:ok, version} =
             Version.new(FavnTestSupport.with_manifest_contract(%Manifest{}),
               manifest_version_id: run.manifest_version_id
             )

    lifecycle = StepAttemptLifecycle.new(run, version, node_key, 0, 2)

    assert {:ok, %{work: work}} = StepAttemptLifecycle.build_work(lifecycle)
    assert work.run_started_at == run.inserted_at
    assert work.required_runner_release_id == run.required_runner_release_id
  end

  defp run_state(opts) do
    RunState.new(
      id: "run_lifecycle_test",
      manifest_version_id: "mv_lifecycle_test",
      manifest_content_hash: "hash_lifecycle_test",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      asset_ref: {MyApp.Assets.Lifecycle, :asset},
      max_attempts: Keyword.get(opts, :max_attempts, 1),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 0)
    )
  end
end
