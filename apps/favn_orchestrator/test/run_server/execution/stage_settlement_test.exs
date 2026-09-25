defmodule FavnOrchestrator.RunServer.Execution.StageSettlementTest do
  @moduledoc "Stage settlement after durable runner result acceptance."

  alias FavnTestSupport.ExecutionDriver

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

    def record_outcomes(command) do
      send(test_pid(), {:resource_outcomes, command})

      case Process.get({__MODULE__, :resource_error}) do
        nil ->
          {:ok, %FavnOrchestrator.Persistence.Results.ResourceCircuitUpdate{closed_resources: []}}

        reason ->
          {:error, reason}
      end
    end

    def finish(command) do
      send(test_pid(), {:materialization_finished, command})

      case Process.get({__MODULE__, :finish_error}) do
        nil ->
          {:ok, %MaterializationDecision{claim_key: command.claim_key, status: command.status}}

        error ->
          {:error, error}
      end
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

    FavnOrchestrator.TestSupport.UnitRunAuthority.start(%RunState{
      id: "run-post-step",
      workspace_id: "workspace-post-step",
      storage_owner_id: "run-owner",
      storage_fencing_token: 1
    })

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
             ExecutionDriver.handle_event(state, {:stage_admission_timeout, timer_token})

    assert next.status == :admission_wait
    refute Map.has_key?(next.admission_timers, timer_token)

    assert [{next_token, %{timer_ref: timer_ref, payload: payload}}] =
             Map.to_list(next.admission_timers)

    assert is_reference(next_token)
    assert payload.kind == :admission_retry
    assert payload.stage_index == 1
    Process.cancel_timer(timer_ref)
  end

  test "a successful node settles exactly once without registration work" do
    fixture = fixture([:a])
    assert {:terminal, finished} = deliver_result(awaiting_state(fixture, [:a]), fixture, :a, :ok)
    assert finished.status == :ok
    assert node_result_count(finished) == 1
    assert_receive {:commit_transition, %{event: %{event_type: :step_finished}}}
    assert_receive {:materialization_finished, %{status: :succeeded, claim_key: "claim-a"}}
    assert_receive {:release_execution_lease, %{lease_id: "lease-a"}}
    refute_receive {:worker_binding_read, _, _}, 20
    refute_receive {:materialization_finished, %{status: :failed}}, 20
  end

  test "siblings settle independently without registration continuations" do
    fixture = fixture([:a, :b, :c])

    assert {:cont, first} =
             deliver_result(awaiting_state(fixture, [:a, :b, :c]), fixture, :a, :ok)

    assert map_size(first.awaits) == 2
    assert first.stage_state.pending_ids == MapSet.new(["rt-b", "rt-c"])
    assert {:cont, second} = deliver_result(first, fixture, :b, :ok)
    assert map_size(second.awaits) == 1
    assert {:terminal, finished} = deliver_result(second, fixture, :c, :ok)
    assert finished.status == :ok
    assert node_result_count(finished) == 3
    refute_receive {:worker_binding_read, _, _}, 20
  end

  for disposition <- [:recover, :exhaust] do
    @tag disposition: disposition
    test "async completed bookkeeping #{disposition} preserves the successful asset", %{
      disposition: disposition
    } do
      fixture = fixture([:a])
      state = awaiting_state(fixture, [:a])
      task = task_id(:a)

      permit = %FavnOrchestrator.Persistence.Results.ResourceCircuitPermit{
        resource: Favn.Resource.Ref.new!(:connection, "warehouse"),
        owner_id: "owner",
        probe?: false
      }

      owned = %{state.awaits[task].entry | resource_circuit_permits: [permit]}
      state = put_in(state.awaits[task].entry, owned)
      state = %{state | work_set: ActiveTaskSet.add_entry(state.work_set, owned)}

      conflict =
        Error.new(:conflict, "history busy",
          retryable?: true,
          details: %{reason_code: "execution_history_owner_busy"}
        )

      Process.put({FakeStore, :resource_error}, conflict)

      assert {:persist_retry, paused, retry, ^conflict} =
               deliver_result(state, fixture, :a, :ok)

      assert_receive {:materialization_finished, %{status: :succeeded}}
      assert_receive {:resource_outcomes, command}
      assert ResultBuilder.latest_node_status(paused.run, fixture.node_keys.a) == :ok
      retry = PersistenceRetry.rejected(retry, conflict)

      result =
        case disposition do
          :recover ->
            Process.delete({FakeStore, :resource_error})

            assert {:ownership_gate, gated, replay} =
                     ExecutionDriver.retry_persistence(paused, retry)

            assert_receive {:resource_outcomes, ^command}
            ExecutionDriver.resume_persisted_retry(gated, replay)

          :exhaust ->
            retry = %{retry | started_ms: System.monotonic_time(:millisecond) - 30_001}
            ExecutionDriver.retry_persistence(paused, retry)
        end

      finished =
        case disposition do
          :recover ->
            assert {:terminal, finished} = result
            assert finished.status == :ok
            finished

          :exhaust ->
            assert {:recovery_required, recovering,
                    %{details: %{reason_code: "persistence_retry_exhausted"}}} = result

            refute_received {:commit_transition, %{event: %{event_type: :step_settled}}}
            recovering.run
        end

      assert ResultBuilder.latest_node_status(finished, fixture.node_keys.a) == :ok
      refute_received {:materialization_finished, _}
      refute_received {:worker_binding_read, _, _}
    end
  end

  test "an ambiguous initial resource settlement response preserves the accepted result" do
    fixture = fixture([:a])
    state = awaiting_state(fixture, [:a])
    task = task_id(:a)

    permit = %FavnOrchestrator.Persistence.Results.ResourceCircuitPermit{
      resource: Favn.Resource.Ref.new!(:connection, "warehouse"),
      owner_id: "owner",
      probe?: false
    }

    owned = %{state.awaits[task].entry | resource_circuit_permits: [permit]}
    state = put_in(state.awaits[task].entry, owned)
    state = %{state | work_set: ActiveTaskSet.add_entry(state.work_set, owned)}
    Process.put({FakeStore, :resource_error}, Error.new(:timeout, "resource outcome reply lost"))

    assert {:recovery_required, recovering,
            {:resource_outcomes_unavailable, _, %{kind: :timeout}}} =
             deliver_result(state, fixture, :a, :ok)

    assert ResultBuilder.latest_node_status(recovering.run, fixture.node_keys.a) == :ok
    refute_received {:commit_transition, %{event: %{event_type: :step_settled}}}
  end

  test "ambiguous materialization finish preserves successful result without settlement" do
    fixture = fixture([:a])
    Process.put({FakeStore, :finish_error}, Error.new(:timeout, "finish reply lost"))

    assert {:recovery_required, recovering, {:post_step_bookkeeping_unavailable, _, _}} =
             deliver_result(awaiting_state(fixture, [:a]), fixture, :a, :ok)

    assert ResultBuilder.latest_node_status(recovering.run, fixture.node_keys.a) == :ok
    assert_receive {:commit_transition, %{event: %{event_type: :step_finished}}}
    refute_received {:commit_transition, %{event: %{event_type: :step_settled}}}
  end

  test "secondary failed-claim settlement preserves the original asset error" do
    fixture = fixture([:a])
    Process.put({FakeStore, :finish_error}, Error.new(:conflict, "write outcome unresolved"))

    assert {:terminal, failed} =
             deliver_result(awaiting_state(fixture, [:a]), fixture, :a, :error)

    assert failed.error.type == :forced_failure
    assert failed.error.details.post_step_failure.operation == :materialization_settlement
    assert failed.error.details.post_step_failure.message =~ "after the asset failed"
  end

  defp deliver_result(state, fixture, name, status) do
    ExecutionDriver.handle_event(
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
