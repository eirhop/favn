defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionNodeFailureTest do
  @moduledoc """
  A terminal admission failure that belongs to one node must not stop its stage.

  The fixture holds node `a` on the runner while node `b` is admitted, so the
  runner result for `a` is delivered only when a test chooses to. Assets are
  non-SQL so no execution package is fetched, and plan nodes carry pinned
  target and evidence generation identities so the claim reaches the store.
  """

  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.Admission
  alias FavnOrchestrator.Persistence.Results.ExecutionLease
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Recovery
  alias FavnOrchestrator.RunState

  @held_task_id "rt_admission_sibling_held"

  @conflict Error.new(:conflict, "target operation is in progress",
              details: %{reason_code: "target_operation_in_progress"}
            )

  @unavailable Error.new(:unavailable, "claim store is unavailable", retryable?: true)

  defmodule FakeStore do
    alias FavnOrchestrator.Persistence.Results.Admission
    alias FavnOrchestrator.Persistence.Results.CapacityRelease
    alias FavnOrchestrator.Persistence.Results.ExecutionLease
    alias FavnOrchestrator.Persistence.Results.MaterializationClaim
    alias FavnOrchestrator.Persistence.Results.MaterializationDecision
    alias FavnOrchestrator.Persistence.Results.RunCommitted
    alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint

    def get_run(_query), do: {:error, :forced_missing}

    def commit_transition(command) do
      send(self(), {:commit_transition, command})

      case Process.get({__MODULE__, :commit_failures}, 0) do
        failures when failures > 0 ->
          Process.put({__MODULE__, :commit_failures}, failures - 1)
          {:error, :forced_commit_failure}

        _none ->
          {:ok,
           %RunCommitted{
             run: command.run,
             event: command.event,
             event_id: 1,
             outbox_event_id: 1,
             replayed?: false
           }}
      end
    end

    def admit(command) do
      send(self(), {:admit_execution, command})

      {:ok,
       %Admission{
         status: :admitted,
         lease: %ExecutionLease{
           workspace_id: command.workspace_context.workspace_id,
           lease_id: command.lease_id,
           run_id: command.run_id,
           step_id: command.step_id,
           owner_id: command.owner_id,
           owner_generation: command.owner_generation,
           status: :held,
           expires_at: DateTime.add(DateTime.utc_now(), 60, :second),
           scope_ids: Enum.map(command.requests, & &1.scope_id)
         }
       }}
    end

    def release_lease(command) do
      send(self(), {:release_execution_lease, command})

      {:ok,
       %CapacityRelease{
         released_lease_ids: [command.lease_id],
         expired_waiter_ids: [],
         freed_scope_ids: []
       }}
    end

    def release_run_leases(command) do
      send(self(), {:release_run_leases, command})

      {:ok, %CapacityRelease{released_lease_ids: [], expired_waiter_ids: [], freed_scope_ids: []}}
    end

    def claim(command) do
      send(self(), {:materialization_claim, command})

      if command.target_id in Process.get({__MODULE__, :claimable_target_ids}, []) do
        {:ok,
         %MaterializationDecision{
           claim_key: command.claim_key,
           status: :claimed,
           claim: %MaterializationClaim{
             workspace_id: command.workspace_context.workspace_id,
             claim_key: command.claim_key,
             deployment_id: command.deployment_id,
             target_kind: command.target_kind,
             target_id: command.target_id,
             target_generation_id: command.target_generation_id,
             evidence_generation_id: command.evidence_generation_id,
             partition_key: command.partition_key,
             run_id: command.run_id,
             owner_id: command.owner_id,
             fencing_token: 1,
             status: :claimed,
             expires_at: DateTime.add(command.occurred_at, 60, :second),
             version: 1
           }
         }}
      else
        {:error, Process.get({__MODULE__, :claim_error})}
      end
    end

    def finish(command) do
      send(self(), {:materialization_finish, command})

      {:ok, %MaterializationDecision{claim_key: command.claim_key, status: command.status}}
    end

    def put_execution_checkpoint(command) do
      send(self(), {:put_execution_checkpoint, command})

      {:ok,
       %RunExecutionCheckpoint{
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

    start_supervised!(
      {PersistenceRuntime,
       %PersistenceRuntime{
         backend: __MODULE__,
         options: [],
         stores: stores
       }}
    )

    Process.put({FavnOrchestrator.TestRunnerTaskStore, :cancellation_recorder}, self())
    Process.put({FakeStore, :claim_error}, @conflict)

    on_exit(fn ->
      Process.delete({FavnOrchestrator.TestRunnerTaskStore, :cancellation_recorder})
    end)

    {:ok, fixture: fixture()}
  end

  test "a node-specific claim conflict fails only its node and leaves the sibling running", %{
    fixture: fixture
  } do
    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}
    assert command.event.data.node_key == fixture.b_key
    assert command.event.data.error == @conflict
    assert command.event.data.retryable? == false
    assert command.event.data.node_result.status == :error
    assert command.event.data.node_result.runner_task_id == nil

    assert command.run.status == :running,
           "the run must stay running while the sibling drains"

    assert command.run.error == nil

    assert ResultBuilder.latest_node_status(awaiting.stage_state.run, fixture.b_key) == :error,
           "the failed node's result must be carried forward on the live run"

    assert_receive {:admit_execution, admit}
    assert_receive {:release_execution_lease, %{lease_id: released_lease_id}}
    assert released_lease_id == admit.lease_id
    refute released_lease_id == "lease-step-a", "the held sibling's lease must not be released"

    refute_received {:runner_task_cancellation_requested, _command}
    assert awaiting.status == :awaiting
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "the held sibling completes and the run fails with the admission error", %{
    fixture: fixture
  } do
    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert {:terminal, failed} =
             Execution.handle_event(
               awaiting,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    refute_received {:runner_task_cancellation_requested, _command}

    assert failed.status == :error
    assert failed.error == @conflict

    statuses = node_statuses(failed)
    assert statuses[fixture.a_key] == :ok
    assert statuses[fixture.b_key] == :error

    assert Map.has_key?(statuses, fixture.d_key),
           "a downstream of the successful sibling must still be attempted"

    assert statuses[fixture.e_key] == :blocked
  end

  test "only the failed node's dependent is blocked in the next stage", %{fixture: fixture} do
    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert {:terminal, _failed} =
             Execution.handle_event(
               awaiting,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    commands = drain_commits()

    blocked = commits(commands, :step_blocked)
    assert Enum.map(blocked, & &1.event.data.node_key) == [fixture.e_key]
    assert hd(blocked).event.data.reason == :upstream_blocked

    # The dependent of the successful sibling reaches admission instead of
    # being blocked, and only then fails on its own claim conflict.
    assert fixture.d_key in Enum.map(commits(commands, :step_failed), & &1.event.data.node_key)
  end

  test "a missing execution package fails only its node and releases its claim", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claimable_target_ids}, [fixture.f_target_id])

    state = %{
      fixture.state
      | stage_state: %{fixture.state.stage_state | deferred_node_keys: [fixture.f_key]}
    }

    assert {:cont, awaiting} = Execution.handle_event(state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}
    assert command.event.data.node_key == fixture.f_key
    assert command.event.data.error == :execution_package_required
    assert command.run.status == :running

    # The claim this node acquired is failed with the admission error.
    assert_receive {:materialization_finish, %{status: :failed}}
    assert_receive {:release_execution_lease, _release}
    refute_received {:runner_task_cancellation_requested, _command}
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "a crash while the stage drains after the failure is not resumable", %{fixture: fixture} do
    assert {:cont, _awaiting} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}

    assert {:ok,
            {:uncertain,
             %{
               reason: :active_stage_outcomes_not_resumable,
               runner_tasks: [@held_task_id]
             }}} = Recovery.disposition(command.run)
  end

  test "a run-wide claim failure still stops the stage and cancels the sibling", %{
    fixture: fixture
  } do
    Process.put({FakeStore, :claim_error}, @unavailable)

    assert {:cont, draining} = Execution.handle_event(fixture.state, :continue)

    assert_receive {:runner_task_cancellation_requested, %{task_id: @held_task_id}}

    assert commits(drain_commits(), :step_failed) == [],
           "a run-wide failure writes no per-node failure"

    assert {:terminal, failed} =
             Execution.handle_event(
               draining,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    assert failed.status == :error
    assert failed.error == @unavailable
  end

  test "a failed failure write resumes and keeps submitting the rest of the stage", %{
    fixture: fixture
  } do
    state = %{
      fixture.state
      | stage_state: %{
          fixture.state.stage_state
          | deferred_node_keys: [fixture.b_key, fixture.c_key]
        }
    }

    Process.put({FakeStore, :commit_failures}, 1)

    assert {:persist_retry, retry_state, %PersistenceRetry{event_type: :step_failed} = retry,
            :forced_commit_failure} = Execution.handle_event(state, :continue)

    assert retry.data.node_key == fixture.b_key
    refute_received {:runner_task_cancellation_requested, _command}

    assert {:cont, awaiting} = Execution.retry_persistence(retry_state, retry)

    failed_node_keys =
      drain_commits() |> commits(:step_failed) |> Enum.map(& &1.event.data.node_key)

    assert fixture.b_key in failed_node_keys

    assert fixture.c_key in failed_node_keys,
           "the remaining node must be submitted after the retry"

    refute_received {:runner_task_cancellation_requested, _command}
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "the initial-stage resume keeps statuses of nodes completed in earlier attempts", %{
    fixture: fixture
  } do
    completed = %{fixture.a_key => :ok}

    failure = %{
      status: :error,
      error: @conflict,
      node_statuses: %{fixture.b_key => :error}
    }

    resume =
      {:node_failed, fixture.run, [], [], MapSet.new(), [], failure, nil, completed}

    retry =
      PersistenceRetry.new(
        fixture.run,
        :step_failed,
        %{node_key: fixture.b_key, error: @conflict},
        {:stage_admission, 1, resume}
      )

    state = %{
      fixture.state
      | stage_state: nil,
        work_set: ActiveTaskSet.from_entries(fixture.run, []),
        awaits: %{},
        await_timers: %{}
    }

    assert {:terminal, failed} = Execution.retry_persistence(state, retry)
    assert failed.error == @conflict

    statuses = node_statuses(failed)
    assert statuses[fixture.e_key] == :blocked

    assert Map.has_key?(statuses, fixture.d_key),
           "the completed sibling's status must survive the resume so its dependent is not blocked"
  end

  defp fixture do
    a_ref = {__MODULE__.Asset, :a}
    b_ref = {__MODULE__.Asset, :b}
    c_ref = {__MODULE__.Asset, :c}
    d_ref = {__MODULE__.Asset, :d}
    e_ref = {__MODULE__.Asset, :e}
    f_ref = {__MODULE__.Asset, :f}

    a_key = {a_ref, nil}
    b_key = {b_ref, nil}
    c_key = {c_ref, nil}
    d_key = {d_ref, nil}
    e_key = {e_ref, nil}
    f_key = {f_ref, nil}

    nodes = %{
      a_key => plan_node(a_ref, a_key, 0, []),
      b_key => plan_node(b_ref, b_key, 0, []),
      c_key => plan_node(c_ref, c_key, 0, []),
      f_key => plan_node(f_ref, f_key, 0, []),
      d_key => plan_node(d_ref, d_key, 1, [a_key]),
      e_key => plan_node(e_ref, e_key, 1, [b_key])
    }

    plan = %Plan{
      target_refs: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
      target_node_keys: [a_key, b_key, c_key, f_key, d_key, e_key],
      topo_order: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
      stages: [[a_ref, b_ref, c_ref, f_ref], [d_ref, e_ref]],
      node_stages: [[a_key, b_key, c_key, f_key], [d_key, e_key]],
      nodes: nodes
    }

    run =
      RunState.new(
        id: "run-admission-sibling-drain",
        workspace_id: "workspace-admission-sibling-drain",
        deployment_id: "deployment-admission-sibling-drain",
        manifest_version_id: "manifest-admission-sibling-drain",
        manifest_content_hash: "sha256:admission-sibling-drain",
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: a_ref,
        target_refs: [a_ref, b_ref, c_ref, f_ref, d_ref, e_ref],
        plan: plan,
        metadata: %{pipeline_execution_policy: %{max_concurrency: 4}}
      )
      |> RunState.transition(status: :running)
      |> RunState.with_storage_fence("run-owner", 1)

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash,
      runner_releases: run.runner_releases
    }

    assets_by_ref =
      Map.new([a_ref, b_ref, c_ref, d_ref, e_ref], fn {module, name} = ref ->
        {ref, %Asset{ref: ref, module: module, name: name}}
      end)

    # `f` is the only SQL asset, and it has no registered package, so its
    # admission reaches the execution-package call site and fails there.
    {f_module, f_name} = f_ref

    assets_by_ref =
      Map.put(assets_by_ref, f_ref, %Asset{
        ref: f_ref,
        module: f_module,
        name: f_name,
        type: :sql
      })

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
      now: ~U[2026-09-03 10:00:00Z]
    }

    decisions =
      Map.new([a_key, b_key, c_key, f_key], fn node_key ->
        {node_key, %{decision: :run, reason: :forced, freshness_key: "latest"}}
      end)

    entry =
      StageEntry.new!(%{
        run_id: run.id,
        asset_step_id: "step-a",
        asset_ref: a_ref,
        node_key: a_key,
        window: nil,
        task_id: @held_task_id,
        assignment_generation: 0,
        runner_pool: "default",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        decision: Map.fetch!(decisions, a_key),
        stage: 0,
        attempt: 1,
        lease: %{
          workspace_id: run.workspace_id,
          lease_id: "lease-step-a",
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
    work_set = ActiveTaskSet.from_entries(run, [entry])
    run = ActiveTaskSet.sync_run_metadata(run, work_set)

    state = %RunExecutionState{
      run: run,
      version: version,
      manifest_index: manifest_index,
      mode: :pipeline,
      work_set: work_set,
      stage_groups: [{0, [a_key, b_key, c_key, f_key]}, {1, [d_key, e_key]}],
      stage_index: 0,
      stage_attempt: 1,
      stage_state:
        StageAttemptState.new(run, [], [entry], [b_key], MapSet.new(), nil, :batch_budget),
      stage_decisions: decisions,
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
        @held_task_id => %{
          pid: nil,
          monitor_ref: nil,
          timeout_token: timeout_token,
          timeout_ref: Process.send_after(self(), :unused_timeout, 60_000),
          entry: entry,
          kind: :pipeline
        }
      },
      await_timers: %{timeout_token => @held_task_id}
    }

    %{
      run: run,
      state: state,
      version: version,
      a_key: a_key,
      b_key: b_key,
      c_key: c_key,
      d_key: d_key,
      e_key: e_key,
      f_key: f_key,
      f_target_id: TargetIdentity.for_asset(f_ref)
    }
  end

  defp plan_node(ref, node_key, stage, upstream) do
    {_module, name} = ref

    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: [],
      stage: stage,
      execution_pool: nil,
      target_id: TargetIdentity.for_asset(ref),
      target_generation_id: nil,
      evidence_generation_id: "generation-#{name}",
      action: :run,
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :default
    }
  end

  defp ok_result(fixture) do
    %RunnerResult{
      run_id: fixture.run.id,
      manifest_version_id: fixture.run.manifest_version_id,
      manifest_content_hash: fixture.run.manifest_content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      status: :ok,
      asset_results: []
    }
  end

  defp node_statuses(%RunState{} = run) do
    run
    |> ResultBuilder.node_results()
    |> Map.new(&{&1.node_key, &1.status})
  end

  defp commits(commands, event_type),
    do: Enum.filter(commands, &(&1.event.event_type == event_type))

  defp drain_commits, do: receive_commits([])

  defp receive_commits(acc) do
    receive do
      {:commit_transition, command} -> receive_commits([command | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end

defmodule FavnOrchestrator.RunServer.Execution.StageAdmissionNodeFailureTest.Asset do
end
