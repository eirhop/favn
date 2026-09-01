defmodule FavnOrchestrator.RunServer.Execution.SequentialTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Version
  alias Favn.Manifest.Index
  alias Favn.Manifest.Asset
  alias Favn.Plan
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.Persistence.Results.RunnerTask
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

      case Process.get({__MODULE__, :commit_transition}) do
        :succeed ->
          {:ok,
           %FavnOrchestrator.Persistence.Results.RunCommitted{
             run: command.run,
             event: command.event,
             event_id: 1,
             outbox_event_id: 1,
             replayed?: false
           }}

        _forced_failure ->
          {:error, :forced_failure}
      end
    end

    def release_lease(command) do
      send(self(), {:release_execution_lease, command})

      {:ok,
       %FavnOrchestrator.Persistence.Results.CapacityRelease{
         released_lease_ids: [command.lease_id],
         expired_waiter_ids: [],
         freed_scope_ids: []
       }}
    end

    def put_execution_checkpoint(command) do
      send(self(), {:put_execution_checkpoint, command})

      {:ok,
       %FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint{
         workspace_id: command.workspace_context.workspace_id,
         run_id: command.run_id,
         owner_id: command.owner_id,
         fencing_token: command.fencing_token,
         checkpoint_version: command.checkpoint_version,
         checkpoint_revision: command.checkpoint_revision,
         checkpoint_sequence: command.checkpoint_sequence,
         stage: command.stage,
         attempt: command.attempt,
         payload: command.payload,
         payload_hash: command.payload_hash,
         updated_at: command.occurred_at
       }}
    end

    def release_run_leases(command) do
      send(self(), {:release_run_leases, command})

      {:ok,
       %FavnOrchestrator.Persistence.Results.CapacityRelease{
         released_lease_ids: [],
         expired_waiter_ids: [],
         freed_scope_ids: []
       }}
    end

    def admit(command) do
      send(self(), {:admit_execution, command})
      {:error, :forced_refill_stop}
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
    start_supervised!({PersistenceRuntime, runtime})

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

  test "pipeline durable unknown outcome releases its task lease after settlement" do
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
      |> RunState.with_storage_fence("run-owner", 1)

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
        lease: %{
          workspace_id: run.workspace_id,
          lease_id: "lease-named-execution-pool",
          owner_id: "owner-named-execution-pool",
          owner_generation: 1,
          scopes: []
        },
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

    freshness_context = %{
      assets_by_ref: %{},
      refresh_policy: %RefreshPolicy{mode: :auto},
      forced_node_keys: MapSet.new(),
      prior_states: %{},
      current_states: %{},
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: ~U[2026-08-28 10:00:00Z]
    }

    state = %RunExecutionState{
      run: run,
      version: version,
      manifest_index: %Index{},
      mode: :pipeline,
      work_set: ActiveTaskSet.from_entries(run, [entry]),
      stage_groups: [{0, [node_key]}],
      stage_index: 0,
      stage_attempt: 1,
      stage_state: StageAttemptState.new(run, [], [entry], [], MapSet.new()),
      freshness_context: freshness_context,
      stage_freshness_context: freshness_context,
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
      status: :error,
      asset_results: [],
      error:
        RunnerError.new(
          type: :transport_outcome_unknown,
          message: "The runner side-effect outcome is unknown",
          retryable?: false,
          outcome: :unknown
        )
    }

    assert {:persist_retry, retry_state,
            %PersistenceRetry{event_type: :step_failed, data: %{asset_ref: ^ref}} = retry,
            :forced_failure} =
             Execution.handle_event(state, {:runner_result, task_id, {:ok, result}})

    assert_receive {:commit_transition, command}
    assert command.event.event_type == :step_failed

    assert_receive {:release_execution_lease, release}
    assert release.lease_id == "lease-named-execution-pool"
    refute_receive {:release_execution_lease, _duplicate}

    Process.put({FakeStore, :commit_transition}, :succeed)
    on_exit(fn -> Process.delete({FakeStore, :commit_transition}) end)

    assert {:terminal, failed} = Execution.retry_persistence(retry_state, retry)
    assert failed.status == :error
    assert failed.error.outcome == :unknown
    assert_receive {:put_execution_checkpoint, checkpoint}
    assert checkpoint.stage == 0
    assert checkpoint.attempt == 1
    assert_receive {:release_run_leases, %{run_id: "run-named-execution-pool"}}
    refute_receive {:release_execution_lease, _duplicate}
  end

  test "pipeline persistence retry refills only after durable success and does not release twice" do
    release_id = FavnTestSupport.runner_release_id()
    completed_ref = {__MODULE__.Asset, :completed}
    deferred_ref = {__MODULE__.Asset, :deferred}
    completed_key = {completed_ref, nil}
    deferred_key = {deferred_ref, nil}
    task_id = "rt_persistence_retry_refill"

    nodes = %{
      completed_key => plan_node(completed_ref, completed_key, "generation-completed"),
      deferred_key => plan_node(deferred_ref, deferred_key, "generation-deferred")
    }

    plan = %Plan{
      target_refs: [completed_ref, deferred_ref],
      target_node_keys: [completed_key, deferred_key],
      topo_order: [completed_ref, deferred_ref],
      stages: [[completed_ref, deferred_ref]],
      node_stages: [[completed_key, deferred_key]],
      nodes: nodes
    }

    run =
      RunState.new(
        id: "run-persistence-retry-refill",
        workspace_id: "workspace-persistence-retry-refill",
        manifest_version_id: "manifest-persistence-retry-refill",
        manifest_content_hash: "sha256:persistence-retry-refill",
        runner_releases: %{"default" => release_id},
        asset_ref: completed_ref,
        target_refs: [completed_ref, deferred_ref],
        plan: plan,
        metadata: %{pipeline_execution_policy: %{max_concurrency: 1}}
      )
      |> RunState.with_storage_fence("run-owner", 1)

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash,
      runner_releases: %{"default" => release_id}
    }

    assets_by_ref = %{
      completed_ref => manifest_asset(completed_ref),
      deferred_ref => manifest_asset(deferred_ref)
    }

    manifest_index = %Index{assets_by_ref: assets_by_ref}

    freshness_context = %{
      assets_by_ref: assets_by_ref,
      refresh_policy: %RefreshPolicy{mode: :auto},
      forced_node_keys: MapSet.new(),
      prior_states: %{},
      current_states: %{},
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: ~U[2026-08-28 10:00:00Z]
    }

    entry =
      StageEntry.new!(%{
        run_id: run.id,
        asset_step_id: "step-persistence-retry-refill",
        asset_ref: completed_ref,
        node_key: completed_key,
        window: nil,
        task_id: task_id,
        assignment_generation: 0,
        runner_pool: "default",
        required_runner_release_id: release_id,
        decision: %{},
        stage: 0,
        attempt: 1,
        lease: %{
          workspace_id: run.workspace_id,
          lease_id: "lease-persistence-retry-refill",
          owner_id: "run-owner",
          owner_generation: 1,
          scopes: []
        },
        materialization_claim: nil,
        execution_pool: nil,
        resource_circuit_permits: [],
        freshness_key: "latest",
        version: version,
        manifest_index: manifest_index,
        freshness_context: freshness_context
      })

    timeout_token = make_ref()
    timeout_ref = Process.send_after(self(), :unused_timeout, 60_000)

    state = %RunExecutionState{
      run: run,
      version: version,
      manifest_index: manifest_index,
      mode: :pipeline,
      work_set: ActiveTaskSet.from_entries(run, [entry]),
      stage_groups: [{0, [completed_key, deferred_key]}],
      stage_index: 0,
      stage_attempt: 1,
      stage_state:
        StageAttemptState.new(run, [], [entry], [deferred_key], MapSet.new(), nil, :blocked),
      stage_decisions: %{deferred_key => %{decision: :run, freshness_key: "latest"}},
      freshness_context: freshness_context,
      stage_freshness_context: freshness_context,
      freshness_checkpoint: %{
        version: 1,
        revision: 1,
        sequence: run.event_seq,
        stage: 0,
        attempt: 1,
        payload_hash: :crypto.hash(:sha256, "checkpoint")
      },
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

    assert {:persist_retry, retry_state, %PersistenceRetry{event_type: :step_finished} = retry,
            :forced_failure} =
             Execution.handle_event(state, {:runner_result, task_id, {:ok, result}})

    assert_receive {:release_execution_lease, %{lease_id: "lease-persistence-retry-refill"}}
    refute_received {:admit_execution, _command}

    Process.put({FakeStore, :commit_transition}, :succeed)
    on_exit(fn -> Process.delete({FakeStore, :commit_transition}) end)

    assert {:terminal, failed} = Execution.retry_persistence(retry_state, retry)
    assert failed.status == :error
    assert_receive {:admit_execution, %{run_id: "run-persistence-retry-refill"}}
    refute_receive {:release_execution_lease, _duplicate}
  end

  test "sequential confirmed-result cleanup keeps one release owner" do
    release_id = FavnTestSupport.runner_release_id()
    task_id = "rt_sequential_cleanup"
    ref = {__MODULE__.Asset, :sequential}
    node_key = {ref, nil}

    run =
      RunState.new(
        id: "run-sequential-cleanup",
        workspace_id: "workspace-sequential-cleanup",
        manifest_version_id: "manifest-sequential-cleanup",
        manifest_content_hash: "sha256:manifest-sequential-cleanup",
        runner_releases: %{"default" => release_id},
        asset_ref: ref,
        target_refs: [ref]
      )

    entry = %{
      run_id: run.id,
      asset_step_id: "step-sequential-cleanup",
      asset_ref: ref,
      node_key: node_key,
      window: nil,
      task_id: task_id,
      required_runner_release_id: release_id,
      stage: 0,
      attempt: 1,
      lease: %{
        workspace_id: run.workspace_id,
        lease_id: "lease-sequential-cleanup",
        owner_id: "owner-sequential-cleanup",
        owner_generation: 1,
        scopes: []
      }
    }

    timeout_token = make_ref()

    state = %RunExecutionState{
      run: run,
      mode: :sequential,
      sequential_refs: [{ref, node_key, 0}],
      work_set: ActiveTaskSet.from_entries(run, [entry]),
      awaits: %{
        task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: make_ref(),
          entry: entry,
          kind: :sequential
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

    assert {:persist_retry, _state, %PersistenceRetry{event_type: :step_finished},
            :forced_failure} =
             Execution.handle_event(state, {:runner_result, task_id, {:ok, result}})

    assert_receive {:commit_transition, command}
    assert command.event.event_type == :step_finished

    assert_receive {:release_execution_lease, release}
    assert release.lease_id == "lease-sequential-cleanup"
    refute_receive {:release_execution_lease, _duplicate}
  end

  test "unconfirmed local await timeout retains the durable task id" do
    task_id = "rt_unconfirmed_timeout"

    run =
      RunState.new(
        id: "run_unconfirmed_timeout",
        workspace_id: "ws_unconfirmed_timeout",
        manifest_version_id: "mv_unconfirmed_timeout",
        manifest_content_hash: "hash_unconfirmed_timeout",
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: {__MODULE__.Asset, :orders}
      )

    entry = %{task_id: task_id, lease: %{lease_id: "lease_unconfirmed_timeout"}}
    work_set = ActiveTaskSet.from_entries(run, [entry])
    run = ActiveTaskSet.sync_run_metadata(run, work_set)
    timeout_token = make_ref()

    state = %RunExecutionState{
      run: run,
      mode: :sequential,
      work_set: work_set,
      accumulated_results: [],
      awaits: %{
        task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: make_ref(),
          entry: entry,
          kind: :sequential
        }
      },
      await_timers: %{timeout_token => task_id}
    }

    assert {:terminal, failed} =
             Execution.handle_event(state, {:attempt_timeout, task_id, timeout_token})

    assert failed.status == :error
    assert failed.error.type == :runner_await_outcome_unconfirmed
    assert ActiveTaskSet.active_runner_task_ids(failed) == [task_id]
  end

  test "local timeout uses an already-completed durable runner result" do
    task_id = "rt_completed_before_timeout"
    release_id = FavnTestSupport.runner_release_id()
    ref = {__MODULE__.Asset, :orders}
    node_key = {ref, nil}

    run =
      RunState.new(
        id: "run_completed_before_timeout",
        workspace_id: "ws_completed_before_timeout",
        manifest_version_id: "mv_completed_before_timeout",
        manifest_content_hash: "hash_completed_before_timeout",
        runner_releases: %{"default" => release_id},
        asset_ref: ref
      )

    result = %RunnerResult{
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash,
      required_runner_release_id: release_id,
      status: :ok,
      asset_results: []
    }

    task = %RunnerTask{
      workspace_id: run.workspace_id,
      task_id: task_id,
      status: :succeeded,
      result: result,
      payload: %RunnerWork{
        run_id: run.id,
        manifest_version_id: run.manifest_version_id,
        manifest_content_hash: run.manifest_content_hash,
        required_runner_release_id: release_id
      }
    }

    Process.put({FavnOrchestrator.TestRunnerTaskStore, :terminal_cancellation_test}, true)
    Process.put({FavnOrchestrator.TestRunnerTaskStore, task_id}, task)

    entry = %{
      task_id: task_id,
      required_runner_release_id: release_id,
      asset_ref: ref,
      node_key: node_key,
      asset_step_id: "step_completed_before_timeout",
      window: nil,
      stage: 0,
      attempt: 1,
      lease: nil
    }

    work_set = ActiveTaskSet.from_entries(run, [entry])
    run = ActiveTaskSet.sync_run_metadata(run, work_set)
    timeout_token = make_ref()

    state = %RunExecutionState{
      run: run,
      mode: :sequential,
      work_set: work_set,
      accumulated_results: [],
      awaits: %{
        task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: make_ref(),
          entry: entry,
          kind: :sequential
        }
      },
      await_timers: %{timeout_token => task_id}
    }

    assert {:persist_retry, _state, %PersistenceRetry{event_type: :step_finished},
            :forced_failure} =
             Execution.handle_event(state, {:attempt_timeout, task_id, timeout_token})

    refute_received {:commit_transition, %{event: %{event_type: :step_timed_out}}}
  end

  defp plan_node(ref, node_key, evidence_generation_id) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: [],
      downstream: [],
      stage: 0,
      execution_pool: nil,
      evidence_generation_id: evidence_generation_id,
      action: :run,
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :default
    }
  end

  defp manifest_asset({module, name} = ref), do: %Asset{ref: ref, module: module, name: name}
end

defmodule FavnOrchestrator.RunServer.Execution.SequentialTest.Asset do
end
