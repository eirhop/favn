defmodule FavnOrchestrator.RunServer.Execution.SequentialTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Version
  alias Favn.Manifest.Index
  alias Favn.Plan
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.Sequential
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunState

  defmodule FakeStore do
    def get_run(_query), do: {:error, :forced_missing}

    def commit_transition(command) do
      send(self(), {:commit_transition, command})
      {:error, :forced_failure}
    end
  end

  setup do
    stores = %Stores{
      registry: FakeStore,
      runs: FakeStore,
      run_submissions: FakeStore,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: FakeStore,
      scheduler: FakeStore,
      admission: FakeStore,
      resource_circuits: FakeStore,
      target_generations: FakeStore,
      target_recovery: FakeStore,
      rebuilds: FakeStore,
      target_operation_locks: FakeStore,
      materialization: FakeStore,
      backfills: FakeStore,
      operator_reads: FakeStore,
      logs: FakeStore,
      identity: FakeStore,
      maintenance: FakeStore
    }

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    assert {:ok, pid} = PersistenceRuntime.start_link(runtime)

    on_exit(fn -> Process.exit(pid, :shutdown) end)

    :ok
  end

  test "pre-submit failures preserve the planned effective window" do
    ref = {__MODULE__.Asset, :orders}
    node_key = {ref, "window:2026-07-20"}

    window = %{
      key: "window:2026-07-20",
      kind: :day,
      start_at: ~U[2026-07-20 00:00:00Z],
      end_at: ~U[2026-07-21 00:00:00Z],
      timezone: "Etc/UTC"
    }

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: window,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: nil,
          action: :run,
          retry_policy: Favn.Retry.Policy.default(),
          retry_policy_source: :default
        }
      },
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }

    run =
      RunState.new(
        id: "run-pre-submit-window",
        workspace_id: "workspace-pre-submit-window",
        deployment_id: "deployment-pre-submit-window",
        manifest_version_id: "manifest-pre-submit-window",
        manifest_content_hash: "sha256:pre-submit-window",
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash
    }

    state = %RunExecutionState{
      run: run,
      version: version,
      manifest_index: %Index{},
      sequential_refs: [{ref, node_key, 0}]
    }

    assert {:persist_retry, ^state, %PersistenceRetry{event_type: :step_failed, data: data},
            _reason} =
             Sequential.continue(state)

    assert data.window == window
    assert data.node_key == node_key

    assert %RunnerError{phase: :pre_submit, outcome: :safe_failure, retryable?: false} =
             data.error

    assert_receive {:commit_transition, command}
    assert command.event.event_type == :step_failed
    assert command.event.data.window == window
  end

  test "pipeline settlement uses its durable runner task release, not its execution pool" do
    release_id = FavnTestSupport.runner_release_id()
    task_id = "rt_named_execution_pool"
    ref = {__MODULE__.Asset, :orders}
    node_key = {ref, nil}

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: :partner_api,
          action: :run,
          retry_policy: Favn.Retry.Policy.default(),
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-named-execution-pool",
        workspace_id: "workspace-named-execution-pool",
        manifest_version_id: "manifest-named-execution-pool",
        manifest_content_hash: "sha256:named-execution-pool",
        runner_releases: %{"default" => release_id},
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash,
      runner_releases: %{"default" => release_id}
    }

    entry =
      StageEntry.new!(%{
        run_id: run.id,
        asset_step_id: "step-named-execution-pool",
        asset_ref: ref,
        node_key: node_key,
        window: nil,
        task_id: task_id,
        assignment_generation: 0,
        runner_pool: "default",
        required_runner_release_id: release_id,
        decision: %{},
        stage: 0,
        attempt: 1,
        lease: nil,
        materialization_claim: nil,
        execution_pool: :partner_api,
        resource_circuit_permits: [],
        freshness_key: "latest",
        version: version,
        manifest_index: %Index{},
        freshness_context: %{}
      })

    timeout_token = make_ref()
    timeout_ref = Process.send_after(self(), :unused_timeout, 60_000)

    state = %RunExecutionState{
      run: run,
      version: version,
      mode: :pipeline,
      work_set: ActiveTaskSet.from_entries(run, [entry]),
      stage_state: StageAttemptState.new(run, [], [entry], [], MapSet.new()),
      awaits: %{
        task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: timeout_ref,
          entry: entry,
          kind: :pipeline
        }
      },
      await_timers: %{timeout_token => task_id}
    }

    result = %RunnerResult{
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      required_runner_release_id: release_id,
      status: :ok,
      asset_results: []
    }

    assert {:persist_retry, _state,
            %PersistenceRetry{event_type: :step_finished, data: %{asset_ref: ^ref}},
            :forced_failure} =
             Execution.handle_event(state, {:runner_result, task_id, {:ok, result}})

    assert_receive {:commit_transition, command}
    assert command.event.event_type == :step_finished
  end
end

defmodule FavnOrchestrator.RunServer.Execution.SequentialTest.Asset do
end
