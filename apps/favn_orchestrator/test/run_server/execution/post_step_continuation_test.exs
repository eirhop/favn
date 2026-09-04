defmodule FavnOrchestrator.RunServer.Execution.PostStepContinuationTest do
  @moduledoc """
  Execution-level tests for post-step reconciliation running in a worker.

  The test process plays the run process: it drives `Execution.handle_event/2`
  and receives the worker's reply as the owner of the supervised task. A fake
  target-generation store holds the worker on its binding read until the test
  releases it, so every wait is deterministic.
  """

  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.CapacityRelease
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.ActiveTaskSet
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunServer.Execution.StageEntry
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunState

  @control_key :post_step_continuation_test_pid

  defmodule FakeStore do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Results.CapacityRelease
    alias FavnOrchestrator.Persistence.Results.MaterializationDecision
    alias FavnOrchestrator.Persistence.Results.RunCommitted
    alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint

    # Returns a stored run only when a test installs one, so external-cancel
    # evidence can be injected mid-stage.
    def get_run(_query) do
      case Application.get_env(:favn_orchestrator, :post_step_continuation_test_run) do
        nil -> {:error, Error.new(:not_found, "run not stored")}
        run -> {:ok, run}
      end
    end

    def commit_transition(command) do
      send(test_pid(), {:commit_transition, command})

      {:ok,
       %RunCommitted{
         run: command.run,
         event: command.event,
         event_id: 1,
         outbox_event_id: 1,
         replayed?: false
       }}
    end

    def put_execution_checkpoint(command) do
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

    def release_lease(command) do
      send(test_pid(), {:release_execution_lease, command})

      {:ok,
       %CapacityRelease{
         released_lease_ids: [command.lease_id],
         expired_waiter_ids: [],
         freed_scope_ids: []
       }}
    end

    def release_run_leases(_command) do
      {:ok, %CapacityRelease{released_lease_ids: [], expired_waiter_ids: [], freed_scope_ids: []}}
    end

    def finish(command) do
      send(test_pid(), {:materialization_finished, command})
      {:ok, %MaterializationDecision{claim_key: command.claim_key, status: command.status}}
    end

    # Runs inside the worker. Holding here keeps the continuation pending until
    # the test decides how the reconciler should answer.
    def get_binding(query) do
      send(test_pid(), {:worker_binding_read, self(), query})

      receive do
        {:binding, :crash} -> raise "forced worker crash"
        {:binding, response} -> response
      end
    end

    def write(_context, _entry, _opts), do: {:ok, []}

    defp test_pid,
      do: Application.fetch_env!(:favn_orchestrator, :post_step_continuation_test_pid)
  end

  setup do
    Application.put_env(:favn_orchestrator, @control_key, self())

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
      {PersistenceRuntime, %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}}
    )

    start_supervised!({Phoenix.PubSub, name: Events.pubsub_name()})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunPostStepSupervisor})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, @control_key)
      Application.delete_env(:favn_orchestrator, :post_step_continuation_test_run)
    end)

    :ok
  end

  test "matching deferred refill timers continue through the admission loop" do
    timer_token = make_ref()

    run =
      RunState.new(
        id: "matching-deferred-refill",
        workspace_id: "workspace-post-step",
        manifest_version_id: "manifest-version",
        manifest_content_hash: "manifest-hash",
        runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
        asset_ref: {__MODULE__, :asset}
      )

    stage_state =
      StageAttemptState.new(
        run,
        [],
        [],
        [{{__MODULE__, :deferred}, nil}],
        MapSet.new(),
        nil,
        :batch_budget
      )

    state = %RunExecutionState{
      status: :admission_wait,
      run: run,
      stage_index: 1,
      stage_state: stage_state,
      stage_admission_deadline_ms: System.monotonic_time(:millisecond) + 5_000,
      admission_waiters: %{"waiter" => %{waiter_id: "waiter"}},
      admission_timers: %{
        timer_token => %{
          timer_ref: make_ref(),
          payload: %{
            kind: :deferred_refill,
            stage_index: 1,
            refill_cause: :batch_budget
          }
        }
      }
    }

    assert {:cont, next} =
             Execution.handle_event(state, {:stage_admission_timeout, timer_token})

    assert next.status == :admission_wait
    refute Map.has_key?(next.admission_timers, timer_token)

    assert [{next_token, %{timer_ref: timer_ref, payload: payload}}] =
             Map.to_list(next.admission_timers)

    assert is_reference(next_token)
    assert payload.kind == :admission_retry
    assert payload.stage_index == 1
    Process.cancel_timer(timer_ref)
  end

  test "a successful node with a pinned generation settles through a worker exactly once" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)

    assert_receive {:commit_transition, %{event: %{event_type: :step_finished}}}
    assert_receive {:materialization_finished, %{status: :succeeded, claim_key: "claim-a"}}
    assert_receive {:release_execution_lease, %{lease_id: "lease-a"}}

    assert pending.awaits == %{}
    assert pending.status == :awaiting
    assert [{ref, %{pid: worker}}] = Map.to_list(pending.post_step_continuations)
    assert is_reference(ref) and is_pid(worker)
    assert pending.stage_state.pending_ids == MapSet.new()
    assert ResultBuilder.latest_node_status(pending.run, fixture.node_keys.a) == :ok
    assert node_result_count(pending.run) == 1

    assert :await ==
             Execution.pipeline_progress_action(
               pending.stage_state,
               RunExecutionState.in_flight_count(pending),
               0
             )

    release_worker(worker, {:ok, %{active_generation_id: "gen-a"}})
    assert_receive {^ref, :ok}

    assert {:terminal, finished} = Execution.handle_event(pending, {:post_step_reply, ref, :ok})
    assert finished.status == :ok
    assert node_result_count(finished) == 1
    refute_receive {:materialization_finished, %{status: :failed}}, 20
  end

  test "cancel intent preserves completed siblings and suppresses their post-step workers" do
    fixture = fixture([:a, :b], claims: [:a, :b])
    state = awaiting_state(fixture, [:a, :b])

    Application.put_env(:favn_orchestrator, :post_step_continuation_test_run, %{
      state.run
      | metadata: Map.put(state.run.metadata, :cancel_requested, true)
    })

    assert {:cont, after_a} = deliver_result(state, fixture, :a, :ok)
    assert after_a.post_step_continuations == %{}
    assert after_a.run.metadata["cancellation_needs_attention"]
    assert map_size(after_a.awaits) == 1
    assert_receive {:materialization_finished, %{status: :succeeded, claim_key: "claim-a"}}

    assert {:terminal, cancelled} = deliver_result(after_a, fixture, :b, :ok)
    assert cancelled.status == :cancelled
    assert node_result_count(cancelled) == 2
    assert_receive {:materialization_finished, %{status: :succeeded, claim_key: "claim-b"}}
    refute_receive {:worker_binding_read, _, _}, 20
    refute_receive {:materialization_finished, %{status: :failed}}, 20
  end

  test "sibling results settle while a continuation is pending and the reply is applied once" do
    fixture = fixture([:a, :b, :c])
    state = awaiting_state(fixture, [:a, :b, :c])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    assert map_size(pending.post_step_continuations) == 1
    assert pending.stage_state.pending_ids == MapSet.new(["rt-b", "rt-c"])
    sequence_after_a = pending.run.event_seq

    assert {:cont, after_b} = deliver_result(pending, fixture, :b, :error)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = failed_commit}
    assert failed_commit.expected_sequence == sequence_after_a

    assert_receive {:commit_transition,
                    %{event: %{event_type: :stage_draining_after_failure}} = draining}

    assert draining.event.data.pending_task_ids == ["rt-c"]
    assert after_b.status == :awaiting
    assert map_size(after_b.awaits) == 1
    assert map_size(after_b.post_step_continuations) == 1
    assert %{status: :error} = after_b.stage_state.terminal_failure

    [{ref, _continuation}] = Map.to_list(after_b.post_step_continuations)
    release_worker(worker, {:ok, %{active_generation_id: "gen-a"}})
    assert_receive {^ref, :ok}

    assert {:cont, after_a} = Execution.handle_event(after_b, {:post_step_reply, ref, :ok})
    assert after_a.post_step_continuations == %{}
    assert after_a.status == :awaiting
    assert node_result_count(after_a.run) == 2
    assert ResultBuilder.latest_node_status(after_a.run, fixture.node_keys.a) == :ok
    assert ResultBuilder.latest_node_status(after_a.run, fixture.node_keys.b) == :error

    assert {:cont, ^after_a} = Execution.handle_event(after_a, {:post_step_reply, ref, :ok})

    assert {:terminal, failed} = deliver_result(after_a, fixture, :c, :ok)
    assert failed.status == :error
    assert node_result_count(failed) == 3
  end

  test "two pending continuations in one stage settle independently" do
    fixture = fixture([:a, :b], claims: [:a, :b])
    state = awaiting_state(fixture, [:a, :b])

    assert {:cont, pending_a} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker_a, _query}
    assert {:cont, pending_both} = deliver_result(pending_a, fixture, :b, :ok)
    assert_receive {:worker_binding_read, worker_b, _query}

    assert map_size(pending_both.post_step_continuations) == 2
    assert pending_both.awaits == %{}

    [ref_a] = refs_for(pending_both, worker_a)
    [ref_b] = refs_for(pending_both, worker_b)

    release_worker(worker_b, {:ok, %{active_generation_id: "gen-b"}})
    assert_receive {^ref_b, :ok}
    assert {:cont, after_b} = Execution.handle_event(pending_both, {:post_step_reply, ref_b, :ok})
    assert Map.keys(after_b.post_step_continuations) == [ref_a]
    assert after_b.status == :awaiting

    release_worker(worker_a, {:ok, %{active_generation_id: "gen-a"}})
    assert_receive {^ref_a, :ok}
    assert {:terminal, finished} = Execution.handle_event(after_b, {:post_step_reply, ref_a, :ok})
    assert finished.status == :ok
    assert node_result_count(finished) == 2
  end

  test "a worker error reply settles the node as a bounded post-step failure" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)

    release_worker(worker, {:error, :binding_unavailable})
    assert_receive {^ref, {:error, reason}}

    assert {:terminal, failed} =
             Execution.handle_event(pending, {:post_step_reply, ref, {:error, reason}})

    assert failed.status == :error

    assert %{
             type: :post_step_persistence_failed,
             reason: {:initial_target_generation_binding_failed, :binding_unavailable}
           } = failed.error
  end

  test "a worker crash settles the node as a post-step failure with a bounded reason" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)

    release_worker(worker, :crash)
    assert_receive {:DOWN, ^ref, :process, ^worker, {%RuntimeError{}, _stack}}

    assert {:terminal, failed} =
             Execution.handle_event(pending, {:post_step_worker_down, ref, :forced})

    assert failed.status == :error

    assert %{type: :post_step_persistence_failed, reason: {:post_step_worker_down, ":forced"}} =
             failed.error
  end

  test "cancel while pending terminates the worker and leaves the completed claim alone" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    assert_receive {:materialization_finished, %{status: :succeeded}}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)
    worker_monitor = Process.monitor(worker)

    assert {:terminal, cancelled} = Execution.cancel(pending, :operator)

    assert cancelled.status == :cancelled
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :shutdown}
    refute_receive {^ref, _late_reply}, 20
    refute_receive {:materialization_finished, %{status: :failed}}, 20
  end

  test "a terminal transition from a sibling settlement terminates the pending worker" do
    fixture = fixture([:a, :b])
    state = awaiting_state(fixture, [:a, :b])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)
    worker_monitor = Process.monitor(worker)

    Application.put_env(
      :favn_orchestrator,
      :post_step_continuation_test_run,
      %{pending.run | status: :cancelled}
    )

    assert {:terminal, cancelled} = deliver_result(pending, fixture, :b, :ok)
    assert cancelled.status == :cancelled
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :shutdown}
    refute_receive {^ref, _late_reply}, 20
    refute_receive {:materialization_finished, %{status: :failed, claim_key: "claim-a"}}, 20
  end

  test "a reply after the workers were stopped is ignored" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, _worker, _query}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)

    stopped = Execution.stop_post_step_workers(pending)
    assert stopped.post_step_continuations == %{}
    assert {:cont, ^stopped} = Execution.handle_event(stopped, {:post_step_reply, ref, :ok})
    assert {:cont, ^stopped} = Execution.handle_event(stopped, {:post_step_worker_down, ref, :x})
  end

  test "an admission deadline firing while a continuation is pending keeps waiting" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}

    timer_token = make_ref()

    timed =
      RunExecutionState.put_admission_timer(pending, timer_token, make_ref(), %{
        kind: :deadline,
        stage_index: 0
      })

    assert {:cont, waiting} =
             Execution.handle_event(timed, {:stage_admission_timeout, timer_token})

    assert waiting.status == :awaiting
    assert map_size(waiting.post_step_continuations) == 1

    assert :await == Execution.pipeline_progress_action(waiting.stage_state, 1, 0)
    assert :await == Execution.post_refill_action([], nil, 1, 0)

    release_worker(worker, {:ok, %{active_generation_id: "gen-a"}})
  end

  test "an admission failure while a continuation is pending waits for it before finalizing" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])

    assert {:cont, pending} = deliver_result(state, fixture, :a, :ok)
    assert_receive {:worker_binding_read, worker, _query}
    [{ref, _continuation}] = Map.to_list(pending.post_step_continuations)

    failed_run =
      RunState.transition(pending.run,
        status: :error,
        error: %{type: :stage_admission_failed, reason: :forced}
      )

    retry =
      PersistenceRetry.new(
        pending.run,
        :step_failed,
        %{},
        {:stage_admission, 1, {:error, failed_run, [], []}}
      )

    assert {:cont, draining} = Execution.retry_persistence(pending, retry)
    assert draining.status == :awaiting
    assert %{status: :error} = draining.terminal_failure
    assert map_size(draining.post_step_continuations) == 1
    assert Process.alive?(worker)

    release_worker(worker, {:ok, %{active_generation_id: "gen-a"}})
    assert_receive {^ref, :ok}

    assert {:terminal, failed} = Execution.handle_event(draining, {:post_step_reply, ref, :ok})
    assert failed.status == :error
    assert node_result_count(failed) == 1
  end

  defp deliver_result(state, fixture, name, status) do
    Execution.handle_event(
      state,
      {:runner_result, task_id(name), {:ok, runner_result(fixture, status)}}
    )
  end

  defp runner_result(fixture, :ok) do
    %RunnerResult{
      run_id: fixture.run.id,
      manifest_version_id: fixture.run.manifest_version_id,
      manifest_content_hash: fixture.run.manifest_content_hash,
      required_runner_release_id: fixture.release_id,
      status: :ok,
      asset_results: []
    }
  end

  defp runner_result(fixture, :error) do
    %{
      runner_result(fixture, :ok)
      | status: :error,
        error:
          RunnerError.new(
            type: :forced_failure,
            message: "forced sibling failure",
            retryable?: false,
            outcome: :safe_failure
          )
    }
  end

  defp release_worker(worker, response), do: send(worker, {:binding, response})

  defp refs_for(state, worker) do
    for {ref, %{pid: ^worker}} <- state.post_step_continuations, do: ref
  end

  defp node_result_count(%RunState{result: %{node_results: results}}), do: length(results)
  defp node_result_count(%RunState{}), do: 0

  defp task_id(name), do: "rt-" <> Atom.to_string(name)

  defp fixture(names, opts \\ []) do
    claims = Keyword.get(opts, :claims, [:a])
    release_id = FavnTestSupport.runner_release_id()

    refs =
      Map.new(
        names,
        &{&1, {Module.concat(__MODULE__.Asset, Macro.camelize(Atom.to_string(&1))), :asset}}
      )

    node_keys = Map.new(refs, fn {name, ref} -> {name, {ref, nil}} end)
    ordered_refs = Enum.map(names, &refs[&1])
    ordered_keys = Enum.map(names, &node_keys[&1])

    plan = %Plan{
      target_refs: ordered_refs,
      target_node_keys: ordered_keys,
      topo_order: ordered_refs,
      stages: [ordered_refs],
      node_stages: [ordered_keys],
      nodes:
        Map.new(names, fn name -> {node_keys[name], plan_node(refs[name], node_keys[name])} end)
    }

    run =
      RunState.new(
        id: "run-post-step",
        workspace_id: "workspace-post-step",
        deployment_id: "deployment-post-step",
        manifest_version_id: "manifest-post-step",
        manifest_content_hash: "sha256:post-step",
        runner_releases: %{"default" => release_id},
        asset_ref: List.first(ordered_refs),
        target_refs: ordered_refs,
        submit_kind: :pipeline,
        plan: plan
      )
      |> Map.put(:event_seq, 3)
      |> Map.put(:status, :running)
      |> RunState.with_storage_fence("run-owner", 1)

    version = %Version{
      manifest_version_id: run.manifest_version_id,
      content_hash: run.manifest_content_hash,
      runner_releases: %{"default" => release_id}
    }

    assets_by_ref =
      Map.new(ordered_refs, fn {module, name} = ref ->
        {ref, %Asset{ref: ref, module: module, name: name}}
      end)

    %{
      run: run,
      version: version,
      release_id: release_id,
      refs: refs,
      node_keys: node_keys,
      claims: claims,
      manifest_index: %Index{assets_by_ref: assets_by_ref},
      freshness_context: %{
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
    }
  end

  defp awaiting_state(fixture, names) do
    entries = Enum.map(names, &entry(fixture, &1))

    awaits =
      Map.new(entries, fn entry ->
        {entry.task_id,
         %{
           pid: nil,
           monitor_ref: nil,
           timeout_token: make_ref(),
           timeout_ref: Process.send_after(self(), :unused_timeout, 60_000),
           entry: entry,
           kind: :pipeline
         }}
      end)

    stage_node_keys = Enum.map(names, &fixture.node_keys[&1])

    %RunExecutionState{
      run: fixture.run,
      version: fixture.version,
      manifest_index: fixture.manifest_index,
      mode: :pipeline,
      status: :awaiting,
      work_set: ActiveTaskSet.from_entries(fixture.run, entries),
      stage_groups: [{0, stage_node_keys}],
      stage_index: 0,
      stage_attempt: 1,
      stage_state: StageAttemptState.new(fixture.run, [], entries, [], MapSet.new()),
      stage_decisions:
        Map.new(stage_node_keys, &{&1, %{decision: :run, freshness_key: "latest"}}),
      freshness_context: fixture.freshness_context,
      stage_freshness_context: fixture.freshness_context,
      freshness_checkpoint: %{
        version: 1,
        revision: 1,
        sequence: fixture.run.event_seq,
        stage: 0,
        attempt: 1,
        payload_hash: :crypto.hash(:sha256, "checkpoint")
      },
      awaits: awaits,
      await_timers: Map.new(awaits, fn {task_id, await} -> {await.timeout_token, task_id} end)
    }
  end

  defp entry(fixture, name) do
    ref = fixture.refs[name]
    node_key = fixture.node_keys[name]
    suffix = Atom.to_string(name)

    StageEntry.new!(%{
      run_id: fixture.run.id,
      asset_step_id: "step-" <> suffix,
      asset_ref: ref,
      node_key: node_key,
      window: nil,
      task_id: task_id(name),
      assignment_generation: 0,
      runner_pool: "default",
      required_runner_release_id: fixture.release_id,
      decision: %{decision: :run, freshness_key: "latest"},
      stage: 0,
      attempt: 1,
      lease: %{
        workspace_id: fixture.run.workspace_id,
        lease_id: "lease-" <> suffix,
        owner_id: "run-owner",
        owner_generation: 1,
        scopes: []
      },
      materialization_claim: if(name in fixture.claims, do: claim(fixture, name), else: nil),
      execution_pool: nil,
      resource_circuit_permits: [],
      freshness_key: "latest"
    })
  end

  defp claim(fixture, name) do
    suffix = Atom.to_string(name)

    %{
      claim_key: "claim-" <> suffix,
      workspace_id: fixture.run.workspace_id,
      run_id: fixture.run.id,
      asset_step_id: "step-" <> suffix,
      node_key: fixture.node_keys[name],
      owner_id: "run-owner",
      fencing_token: 1,
      version: 1,
      status: :claimed,
      target_generation_id: "gen-" <> suffix,
      evidence_generation_id: "evidence-" <> suffix,
      manifest_version_id: fixture.run.manifest_version_id,
      manifest_content_hash: fixture.run.manifest_content_hash
    }
  end

  defp plan_node(ref, node_key) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: [],
      downstream: [],
      stage: 0,
      execution_pool: nil,
      evidence_generation_id:
        "evidence:" <> (node_key |> :erlang.phash2() |> Integer.to_string()),
      action: :run,
      retry_policy: Favn.Retry.Policy.default(),
      retry_policy_source: :default
    }
  end
end
