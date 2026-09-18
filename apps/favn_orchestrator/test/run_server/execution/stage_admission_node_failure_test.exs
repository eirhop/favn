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
  alias FavnOrchestrator.RunState

  @held_task_id "rt_admission_sibling_held"

  @conflict Error.new(:conflict, "target operation is in progress",
              details: %{reason_code: "target_operation_in_progress"}
            )

  @later_conflict Error.new(:conflict, "a different target operation is in progress",
                    details: %{reason_code: "target_operation_in_progress"}
                  )

  defmodule FakeStore do
    alias FavnOrchestrator.Persistence.Results.{
      RunCommitted,
      RunExecutionCheckpoint,
      CapacityRelease
    }

    def get_run(_), do: {:error, :forced_missing}

    def get_execution_package(_),
      do: {:error, Error.new(:unavailable, "package read unavailable")}

    def commit_transition(command) do
      send(self(), {:commit_transition, command})

      case Process.get({__MODULE__, :commit_results}, []) do
        [result | rest] ->
          Process.put({__MODULE__, :commit_results}, rest)
          result

        [] ->
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
      send(self(), {:runner_admission, command})
      errors = Process.get({__MODULE__, :admission_errors}, %{})

      {:error,
       Map.get(errors, command.claim.target_id, Process.get({__MODULE__, :admission_error}))}
    end

    def request_cancellation(command) do
      send(self(), {:runner_task_cancellation_requested, command})
      {:error, :unexpected_cancellation}
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

    def put_execution_checkpoint(command) do
      {:ok,
       struct(
         RunExecutionCheckpoint,
         Map.from_struct(command)
         |> Map.take([
           :run_id,
           :owner_id,
           :fencing_token,
           :checkpoint_version,
           :checkpoint_revision,
           :checkpoint_sequence,
           :stage,
           :attempt,
           :payload,
           :payload_hash
         ])
         |> Map.merge(%{
           workspace_id: command.workspace_context.workspace_id,
           updated_at: command.occurred_at
         })
       )}
    end
  end

  setup do
    stores = struct(Stores, Map.new(Map.keys(Map.from_struct(struct(Stores))), &{&1, FakeStore}))

    start_supervised!(
      {PersistenceRuntime, %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}}
    )

    Process.put({FakeStore, :admission_error}, @conflict)
    {:ok, fixture: fixture()}
  end

  test "classification retry preserves its committed decision and remaining nodes", %{
    fixture: fixture
  } do
    alias FavnOrchestrator.RunServer.Execution.StageClassifier

    conflict =
      Error.new(:conflict, "history busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :commit_results}, [{:error, conflict}])

    context = %{
      fixture.state.freshness_context
      | upstream_statuses: %{fixture.a_key => :error, fixture.b_key => :ok}
    }

    assert {:persist_retry, retry, ^conflict} =
             StageClassifier.classify(
               fixture.run,
               fixture.version,
               1,
               [fixture.d_key, fixture.e_key],
               context,
               nil
             )

    assert retry.event_type == :step_blocked
    assert_receive {:commit_transition, original}
    assert :ok = PersistenceRetry.persist(retry)
    assert_receive {:commit_transition, ^original}
    {:stage_classification, continuation} = retry.resume

    assert {:ok, run, [], _, context, _, [remaining]} =
             StageClassifier.resume_persisted(continuation, continuation.persisted_run)

    assert remaining == fixture.e_key
    assert run.event_seq == original.event.sequence
    assert context.upstream_statuses[fixture.d_key] == :blocked
    assert ResultBuilder.latest_node_status(run, fixture.d_key) == :blocked
    refute_received {:commit_transition, _duplicate}
  end

  test "a rejected atomic admission fails only its node and leaves the sibling running", %{
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

    assert_receive {:runner_admission, _command}
    refute_received {:release_execution_lease, _release}

    refute_received {:runner_task_cancellation_requested, _command}
    assert awaiting.status == :awaiting
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "the held sibling completes and the run fails with the admission error", %{
    fixture: fixture
  } do
    # `d` fails on its own distinguishable conflict in the next stage, so the
    # terminal error proves the FIRST failure wins rather than the last one.
    Process.put({FakeStore, :admission_errors}, %{fixture.d_target_id => @later_conflict})

    assert {:cont, awaiting} = Execution.handle_event(fixture.state, :continue)

    assert {:terminal, failed} =
             Execution.handle_event(
               awaiting,
               {:runner_result, @held_task_id, {:ok, ok_result(fixture)}}
             )

    refute_received {:runner_task_cancellation_requested, _command}

    assert failed.status == :error

    assert failed.error == @conflict,
           "the first terminal failure must stay the run error, not the later one"

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

  test "a missing package fails before acquiring any claim or capacity", %{
    fixture: fixture
  } do
    state = %{
      fixture.state
      | stage_state: %{fixture.state.stage_state | deferred_node_keys: [fixture.f_key]}
    }

    assert {:cont, awaiting} = Execution.handle_event(state, :continue)

    assert_receive {:commit_transition, %{event: %{event_type: :step_failed}} = command}
    assert command.event.data.node_key == fixture.f_key
    assert command.event.data.error == :execution_package_required
    assert command.run.status == :running

    refute_received {:runner_admission, _command}
    refute_received {:release_execution_lease, _release}
    refute_received {:runner_task_cancellation_requested, _command}
    assert RunExecutionState.in_flight_count(awaiting) == 1
  end

  test "a temporary package read failure preserves the healthy sibling", %{fixture: f} do
    ref = elem(f.f_key, 0)

    index = %{
      f.state.manifest_index
      | assets_by_ref:
          Map.update!(
            f.state.manifest_index.assets_by_ref,
            ref,
            &%{&1 | execution_package_hash: String.duplicate("a", 64)}
          )
    }

    state = %{
      f.state
      | manifest_index: index,
        stage_state: %{f.state.stage_state | deferred_node_keys: [f.f_key]}
    }

    assert {:recovery_required, recovering, %{kind: :unavailable}} =
             Execution.handle_event(state, :continue)

    assert @held_task_id in ActiveTaskSet.task_ids(recovering.work_set)
    refute_received {:runner_admission, _}
    refute_received {:runner_task_cancellation_requested, _}
    refute_received {:release_execution_lease, _}
    refute_received {:commit_transition, %{event: %{event_type: :step_failed}}}
  end

  test "retryable admission reuses its command and preserves the healthy sibling", %{fixture: f} do
    busy =
      Error.new(:conflict, "history busy",
        retryable?: true,
        details: %{reason_code: "execution_history_owner_busy"}
      )

    Process.put({FakeStore, :admission_error}, busy)
    assert {:persist_retry, paused, retry, ^busy} = Execution.handle_event(f.state, :continue)
    assert retry.event_type == :runner_admission
    assert_receive {:runner_admission, original}
    assert {:error, ^busy} = PersistenceRetry.persist(retry)
    assert_receive {:runner_admission, ^original}
    assert original.enqueue.deadline_at == original.intent.deadline_at
    assert @held_task_id in ActiveTaskSet.task_ids(paused.work_set)
    refute_received {:runner_task_cancellation_requested, _}
    refute_received {:release_execution_lease, _}
  end

  for boundary <- [:intent, :admission, :classification] do
    @tag boundary: boundary
    test "uncertain #{boundary} persistence remains recoverable without a retryable flag", %{
      fixture: f,
      boundary: boundary
    } do
      error = Error.new(:timeout, "committed reply may be lost")

      if boundary == :admission,
        do: Process.put({FakeStore, :admission_error}, error),
        else: Process.put({FakeStore, :commit_results}, [{:error, error}, {:error, error}])

      {paused, retry} =
        if boundary == :classification do
          context = %{
            f.state.freshness_context
            | upstream_statuses: %{f.a_key => :error, f.b_key => :ok}
          }

          assert {:persist_retry, retry, ^error} =
                   FavnOrchestrator.RunServer.Execution.StageClassifier.classify(
                     f.run,
                     f.version,
                     1,
                     [f.d_key, f.e_key],
                     context,
                     nil
                   )

          {f.state, retry}
        else
          assert {:persist_retry, paused, retry, ^error} =
                   Execution.handle_event(f.state, :continue)

          {paused, retry}
        end

      assert {:recovery_required, recovering, _} =
               Execution.retry_persistence(paused, PersistenceRetry.rejected(retry, error))

      assert @held_task_id in ActiveTaskSet.task_ids(recovering.work_set)
      refute_received {:runner_task_cancellation_requested, _}
      refute_received {:release_execution_lease, _}
      refute_received {:commit_transition, %{event: %{event_type: :step_failed}}}
    end
  end

  test "a lost intent reply replays that exact transition before admission", %{fixture: f} do
    busy = Error.new(:unavailable, "lost reply", retryable?: true)
    Process.put({FakeStore, :commit_results}, [{:error, busy}])
    assert {:persist_retry, paused, retry, ^busy} = Execution.handle_event(f.state, :continue)
    assert retry.event_type == :step_intended
    assert_receive {:commit_transition, original}
    refute_received {:runner_admission, _}
    assert {:ownership_gate, gated, replay} = Execution.retry_persistence(paused, retry)
    assert_receive {:commit_transition, ^original}
    assert {:cont, resumed} = Execution.resume_persisted_retry(gated, replay)
    assert_receive {:runner_admission, _}
    assert @held_task_id in ActiveTaskSet.task_ids(resumed.work_set)
    refute_received {:runner_task_cancellation_requested, _}
    refute_received {:release_execution_lease, _}
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
        manifest_content_hash: String.duplicate("a", 64),
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
        {node_key,
         %{decision: :run, reason: :forced, node_key: node_key, freshness_key: "latest"}}
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
      f_target_id: TargetIdentity.for_asset(f_ref),
      d_target_id: TargetIdentity.for_asset(d_ref),
      entry: entry
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
