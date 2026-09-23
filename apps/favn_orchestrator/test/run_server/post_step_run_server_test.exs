defmodule FavnOrchestrator.RunServer.PostStepRunServerTest do
  @moduledoc """
  Run-server-level proof that post-step reconciliation no longer blocks the run
  process.

  A real `RunServer` resumes one recovered pipeline run whose asset task is
  already running. The harness replaces every store with in-memory fakes behind
  the persistence runtime and gates the runner-task store so the relation
  inspection the reconciler needs stays queued until the test releases it. While
  it is held, the test drives ownership renewals, cancellation, and fenced
  writes through the managed run lifecycle. Renewal runs independently of the coordinator.
  """

  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.RelationRef
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunnerTaskResultRouter
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.PostStepRunServerTest.HarnessStore
  alias FavnOrchestrator.RunState

  @ref {__MODULE__.MonthlyOrders, :asset}
  @node_key {@ref, nil}
  @asset_task_id "rt-asset"
  @fencing_token 7

  defmodule HarnessStore do
    @moduledoc false

    alias Favn.Contracts.GenerationCapabilitiesResult
    alias Favn.Contracts.RelationInspectionResult
    alias Favn.Contracts.RunnerTask.PersistenceCodec
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Results.CapacityRelease
    alias FavnOrchestrator.Persistence.Results.MaterializationDecision
    alias FavnOrchestrator.Persistence.Results.RunCommitted
    alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint
    alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
    alias FavnOrchestrator.Persistence.Results.RunnerTask
    alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership

    def start(run, opts) do
      Agent.start_link(fn ->
        %{
          run: run,
          tasks: %{},
          commits: [],
          held_task_kinds: Keyword.get(opts, :held_task_kinds, [:relation_inspection]),
          commit_failures: Keyword.get(opts, :commit_failures, %{}),
          renew_result: Keyword.get(opts, :renew_result, :ok),
          test_pid: Keyword.fetch!(opts, :test_pid)
        }
      end)
    end

    def put_task(task), do: Agent.update(agent(), &put_in(&1, [:tasks, task.task_id], task))
    def commits, do: Agent.get(agent(), & &1.commits) |> Enum.reverse()
    def latest_run, do: Agent.get(agent(), & &1.run)

    # runs

    def get_run(_query) do
      case Agent.get(agent(), &Map.get(&1, :read_failure)) do
        nil -> {:ok, latest_run()}
        reason -> {:error, reason}
      end
    end

    def require_diagnosis(command) do
      notify({:diagnosis_required, command.reason_code, command.fencing_token})
      :ok
    end

    def page_events(query) do
      run = latest_run()

      step_id =
        FavnOrchestrator.AssetStepIdentity.asset_step_id(
          run.id,
          {run.asset_ref, nil},
          run.asset_ref
        )

      events =
        for {kind, sequence} <- [
              run_submitted: 1,
              run_started: 2,
              run_execution_position: 3,
              step_started: 4
            ] do
          FavnOrchestrator.Projector.run_event(%{run | event_seq: sequence}, kind, %{
            asset_step_id: step_id,
            stage: 0,
            attempt: 1,
            runner_task_id: "rt-asset",
            position: %{
              "version" => 1,
              "mode" => "pipeline",
              "index" => 0,
              "attempt" => 1,
              "phase" => "admit"
            }
          })
        end

      {:ok, %{items: Enum.filter(events, &(&1.sequence > query.after_sequence))}}
    end

    def commit_transition(command) do
      event_type = command.event.event_type

      case Agent.get_and_update(agent(), &record_commit(&1, command)) do
        {:fail, :history_busy} ->
          notify({:run_transition_held, event_type})

          {:error,
           Error.new(:conflict, "history busy",
             retryable?: true,
             details: %{reason_code: "execution_history_owner_busy"}
           )}

        {:fail, :fenced} ->
          {:error, Error.new(:fenced, "run ownership fencing token is stale")}

        {:fail, :cancel_once} ->
          {:error, Error.new(:conflict, "cancellation won the snapshot sequence")}

        {:fail, :unavailable_then_cancel} ->
          {:error, Error.new(:unavailable, "temporary test failure")}

        :ok ->
          notify({:run_transition_committed, event_type})

          {:ok,
           %RunCommitted{
             run: command.run,
             event: command.event,
             event_id: System.unique_integer([:positive, :monotonic]),
             outbox_event_id: 1,
             replayed?: false
           }}
      end
    end

    defp record_commit(state, command) do
      event_type = command.event.event_type
      commits = [%{event_type: event_type, run: command.run} | state.commits]

      case Map.get(state.commit_failures, event_type) do
        nil ->
          {:ok, %{state | commits: commits, run: command.run}}

        :cancel_once ->
          requested =
            FavnOrchestrator.RunState.transition(state.run,
              metadata: Map.put(state.run.metadata, :cancel_requested, true)
            )

          {{:fail, :cancel_once},
           %{
             state
             | commits: commits,
               run: requested,
               commit_failures: Map.delete(state.commit_failures, event_type)
           }}

        :unavailable_then_cancel ->
          {{:fail, :unavailable_then_cancel},
           %{
             state
             | commits: commits,
               commit_failures: Map.put(state.commit_failures, event_type, :cancel_once)
           }}

        failure ->
          {{:fail, failure}, %{state | commits: commits}}
      end
    end

    def get_execution_checkpoint(_query), do: {:ok, Agent.get(agent(), & &1.checkpoint)}

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

    # run ownership

    def get_deployment_manifest(_query), do: {:ok, Agent.get(agent(), & &1.version)}
    def maintain_targets(_, _, _, _), do: {:ok, %{}}
    def claim_run(command), do: {:ok, ownership(command)}

    def renew_run(command) do
      result =
        Agent.get_and_update(agent(), fn state ->
          case state.renew_result do
            [result | rest] -> {result, %{state | renew_result: rest}}
            result -> {result, state}
          end
        end)

      notify({:ownership_renewal_attempt, command.renewal_id, result})

      case result do
        :ok ->
          notify({:ownership_renewed, command.fencing_token})
          {:ok, ownership(command)}

        :busy ->
          {:error,
           Error.new(:conflict, "execution history owner is busy",
             retryable?: true,
             details: %{reason_code: "execution_history_owner_busy"}
           )}

        :fenced ->
          notify({:ownership_renewal_rejected, command.fencing_token})
          {:error, Error.new(:fenced, "run ownership fencing token is stale")}
      end
    end

    def release_run(command) do
      notify({:ownership_released, command.fencing_token})
      :ok
    end

    defp ownership(command) do
      %Ownership{
        workspace_id: command.workspace_context.workspace_id,
        run_id: command.run_id,
        owner_id: command.owner_id,
        fencing_token: Map.get(command, :fencing_token) || 7,
        expires_at: DateTime.add(DateTime.utc_now(), 120, :second),
        database_observed_at: DateTime.utc_now()
      }
    end

    # runner tasks

    def get(query) do
      case Agent.get(agent(), &Map.get(&1.tasks, query.task_id)) do
        %RunnerTask{} = task -> {:ok, task}
        nil -> {:error, Error.new(:not_found, "runner task not found")}
      end
    end

    # Runs inside the reconcile worker. A held kind blocks here until the test
    # releases it, which is the queued inspection task from the incident.
    def enqueue(command) do
      held? = command.task_kind in Agent.get(agent(), & &1.held_task_kinds)

      if held? do
        notify({:runner_task_held, command.task_kind, self()})

        receive do
          :release_runner_task -> :ok
        end
      end

      {:ok, payload} =
        PersistenceCodec.decode_payload(
          command.task_kind,
          command.payload,
          Agent.get(agent(), & &1.version)
        )

      task = completed_task(command, payload)
      put_task(task)
      {:ok, task}
    end

    def request_cancellation(command) do
      notify({:runner_task_cancel_requested, command.task_id})

      case get(%{task_id: command.task_id}) do
        {:ok, task} ->
          cancelled = %{task | status: :cancelled}
          put_task(cancelled)
          {:ok, cancelled}

        error ->
          error
      end
    end

    def ensure_demand(command) do
      {:ok,
       %RunnerCapacityDemand{
         runner_pool: command.runner_pool,
         required_runner_release_id: command.required_runner_release_id,
         outstanding_count: 0,
         queued_count: 0,
         active_count: 0,
         version: 0,
         updated_at: command.occurred_at,
         healthy?: true
       }}
    end

    defp completed_task(command, payload) do
      result =
        case {command.task_kind, payload} do
          {:relation_inspection, request} ->
            %RelationInspectionResult{
              asset_ref: request.asset_ref,
              required_runner_release_id: request.required_runner_release_id,
              relation: %{catalog: nil, schema: "analytics", name: "monthly_orders", type: :table},
              columns: [%{name: "id", data_type: "BIGINT", nullable?: false}],
              table_metadata: %{},
              adapter: FavnTestSupport.TargetAdapter,
              inspected_at: ~U[2026-09-03 12:00:00Z]
            }

          {:generation_capabilities, _request} ->
            %GenerationCapabilitiesResult{capabilities: %{transactional_ddl: :unsupported}}
        end

      %RunnerTask{
        workspace_id: command.workspace_context.workspace_id,
        task_id: command.task_id,
        domain_identity: command.domain_identity,
        task_kind: command.task_kind,
        runner_pool: command.runner_pool,
        required_runner_release_id: command.required_runner_release_id,
        required_capability: command.required_capability,
        retry_class: :terminal,
        payload: payload,
        payload_hash: command.payload_hash,
        orchestration_context: command.orchestration_context,
        assignment_generation: 0,
        status: :succeeded,
        result: result,
        inserted_at: command.occurred_at
      }
    end

    # admission

    def release_lease(command) do
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

    # materialization and target generations

    def finish(command) do
      notify({:materialization_finished, command.status, command.claim_key})
      {:ok, %MaterializationDecision{claim_key: command.claim_key, status: command.status}}
    end

    def get_binding(_query) do
      {:ok,
       %{
         active_generation_id: nil,
         compatibility_status: :uninitialized,
         desired_manifest_id: latest_run().manifest_version_id
       }}
    end

    def reconcile_initial(command) do
      notify({:reconcile_initial, command.target_generation_id})
      {:ok, :reconciled}
    end

    def get_evidence_bindings(query) do
      {:ok,
       Enum.map(query.target_ids, fn target_id ->
         %{target_id: target_id, evidence_generation_id: "ag_" <> target_id}
       end)}
    end

    # freshness and logs

    def get_freshness_many(_query), do: {:ok, []}

    defp notify(message), do: send(Agent.get(agent(), & &1.test_pid), message)

    defp agent, do: Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent)
  end

  setup context do
    fixture = fixture()
    store_opts = Map.get(context, :store_opts, [])

    {:ok, agent} = HarnessStore.start(fixture.run, Keyword.put(store_opts, :test_pid, self()))

    Agent.update(
      agent,
      &Map.merge(&1, %{version: fixture.version, checkpoint: checkpoint(fixture)})
    )

    Application.put_env(:favn_orchestrator, :post_step_run_server_agent, agent)
    HarnessStore.put_task(running_asset_task(fixture))

    stores = %Stores{
      registry: HarnessStore,
      runs: HarnessStore,
      run_submissions: HarnessStore,
      runner_tasks: HarnessStore,
      run_ownership: HarnessStore,
      scheduler: HarnessStore,
      admission: HarnessStore,
      resource_circuits: HarnessStore,
      target_generations: HarnessStore,
      target_recovery: HarnessStore,
      rebuilds: HarnessStore,
      target_operation_locks: HarnessStore,
      materialization: HarnessStore,
      backfills: HarnessStore,
      operator_reads: HarnessStore,
      logs: HarnessStore,
      identity: HarnessStore,
      maintenance: HarnessStore
    }

    start_supervised!(
      {PersistenceRuntime, %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}}
    )

    start_supervised!({Phoenix.PubSub, name: Events.pubsub_name()})
    FavnOrchestrator.TestSupport.ManagedRun.ensure_started()
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerTaskWaitSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})

    on_exit(fn -> Application.delete_env(:favn_orchestrator, :post_step_run_server_agent) end)

    {:ok, fixture: fixture}
  end

  test "replacement has its own handoff deadline after the old stop acknowledgement timed out", %{
    fixture: fixture
  } do
    manager = FavnOrchestrator.RunManager
    old_id = make_ref()
    key = {fixture.run.workspace_id, fixture.run.id}

    old =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> Process.exit(old, :kill) end)

    :sys.replace_state(manager, fn state ->
      %{
        state
        | lifecycles: %{
            old_id => %{
              key: key,
              phase: :stopping,
              ownership: nil,
              coordinator: nil,
              preparer: nil,
              keeper: nil,
              maintenance: nil,
              maintenance_ready?: false,
              diagnostic_reason: nil,
              waiting: [],
              pids: MapSet.new([old])
            }
          }
      }
    end)

    ownership = %Ownership{
      workspace_id: fixture.run.workspace_id,
      run_id: fixture.run.id,
      owner_id: "run-owner",
      fencing_token: @fencing_token,
      expires_at: DateTime.add(DateTime.utc_now(), 120, :second),
      database_observed_at: DateTime.utc_now()
    }

    context =
      FavnOrchestrator.Persistence.SystemContext.workspace(
        fixture.run.workspace_id,
        :run_recovery
      )

    send(manager, {:stop_ack_timeout, old_id})
    assert :sys.get_state(manager).lifecycles[old_id].phase == :stopping
    assert {:ok, _} = manager.recover_claimed_run(context, ownership)
    entry = :sys.get_state(manager).lifecycles |> Map.delete(old_id) |> Map.values() |> hd()
    preparer_monitor = Process.monitor(entry.preparer)
    keeper_monitor = Process.monitor(entry.keeper)
    assert entry.coordinator == nil

    assert_receive {:diagnosis_required, "prior_generation_stop_unconfirmed", @fencing_token},
                   12_000

    assert_receive {:DOWN, ^preparer_monitor, :process, _, _}, 2_000
    assert_receive {:DOWN, ^keeper_monitor, :process, _, _}, 2_000

    assert HarnessStore.latest_run().metadata["recovery_attention"]["phase"] ==
             "prior_generation_stop_unconfirmed"

    assert :sys.get_state(manager).run_pids == %{}
    assert :sys.get_state(manager).lifecycles[old_id].phase == :stopping
  end

  test "a blocked activation does not hold the run supervisor or manager", %{fixture: fixture} do
    {healthy, _} = start_run(fixture)
    healthy_keeper = :sys.get_state(healthy).lease_keeper
    observer = self()

    blocked_keeper =
      spawn(fn ->
        receive do
          {:"$gen_call", _from, {:transfer, _pid}} ->
            send(observer, :transfer_blocked)

            receive do
              :stop -> :ok
            end
        end
      end)

    on_exit(fn -> Process.exit(blocked_keeper, :kill) end)

    args = %{
      run_state: fixture.run,
      version: fixture.version,
      lease_keeper: blocked_keeper,
      storage_ownership: :sys.get_state(healthy).storage_ownership
    }

    assert {:ok, inert} =
             DynamicSupervisor.start_child(
               FavnOrchestrator.RunSupervisor,
               %{
                 id: :blocked_activation,
                 start: {RunServer, :start_link, [args]},
                 restart: :temporary
               }
             )

    refute_receive :transfer_blocked, 20
    send(inert, :activate)
    assert_receive :transfer_blocked
    assert {:ok, _} = GenServer.call(FavnOrchestrator.RunManager, :active_runs, 200)
    assert is_list(DynamicSupervisor.which_children(FavnOrchestrator.RunSupervisor))
    send(healthy_keeper, :renew)
    assert_receive {:ownership_renewed, @fencing_token}, 1_000
    DynamicSupervisor.terminate_child(FavnOrchestrator.RunSupervisor, inert)
  end

  test "ownership renews while the inspection task is held and the run then completes", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:run_transition_committed, :step_finished}, 5_000
    assert_receive {:materialization_finished, :succeeded, "claim-asset"}
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000

    for _renewal <- 1..3 do
      send(:sys.get_state(pid).lease_keeper, :renew)
      assert_receive {:ownership_renewed, @fencing_token}, 1_000
    end

    assert Process.alive?(pid)
    refute_receive {:run_transition_committed, :run_finished}, 20

    send(worker, :release_runner_task)

    assert_receive {:reconcile_initial, "gen-asset"}, 5_000
    assert_receive {:run_transition_committed, :run_finished}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    assert_receive {:ownership_released, @fencing_token}

    assert HarnessStore.latest_run().status == :ok
    assert [_node_result] = HarnessStore.latest_run().result.node_results
  end

  @tag store_opts: [renew_result: [:busy, :ok]]
  test "retryable ownership contention replays one renewal id inside the live lease", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000

    send(:sys.get_state(pid).lease_keeper, :renew)

    assert_receive {:ownership_renewal_attempt, renewal_id, :busy}, 1_000
    send(:sys.get_state(pid).lease_keeper, :renew)
    assert_receive {:ownership_renewal_attempt, ^renewal_id, :ok}, 2_000
    assert_receive {:ownership_renewed, @fencing_token}, 1_000
    assert Process.alive?(pid)

    send(worker, :release_runner_task)
    assert_receive {:run_transition_committed, :run_finished}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
  end

  for failure <- [:cancel_once, :unavailable_then_cancel] do
    @tag store_opts: [held_task_kinds: [], commit_failures: %{run_finished: failure}]
    test "terminal persistence retains its completed result after #{failure}", %{fixture: fixture} do
      {pid, monitor} = start_run(fixture)
      complete_asset_task(fixture)
      assert_receive {:run_transition_committed, :run_finished}, 5_000
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
      assert HarnessStore.latest_run().status == :ok
      assert [%{status: :ok}] = HarnessStore.latest_run().result.node_results
      assert HarnessStore.latest_run().metadata[:cancel_requested]
      assert_receive {:materialization_finished, :succeeded, "claim-asset"}
      refute_receive {:materialization_finished, _, _}, 20
    end
  end

  test "recovery attention reports persistence and survives ordinary progress", %{
    fixture: fixture
  } do
    alias FavnOrchestrator.RunServer.RecoveryAttention
    run = HarnessStore.latest_run()

    reason =
      Error.new(:timeout, "Persistence retry budget exhausted",
        details: %{
          operation: :resource_outcomes,
          original_error: Error.new(:unavailable, "reply lost")
        }
      )

    assert :saved = RecoveryAttention.record(run, reason)
    saved = HarnessStore.latest_run()
    assert saved.metadata["recovery_attention"]["phase"] == "resource_outcomes"
    assert saved.event_seq == run.event_seq + 1
    assert :already_saved = RecoveryAttention.record(run, reason)
    assert HarnessStore.latest_run() == saved
    another_phase = %{reason | details: %{reason.details | operation: :step_finished}}
    assert :saved = RecoveryAttention.record(saved, another_phase)
    assert HarnessStore.latest_run().metadata["recovery_attention"]["reports"] == 2

    assert HarnessStore.latest_run().metadata["recovery_attention"]["first_reason"] ==
             saved.metadata["recovery_attention"]["first_reason"]

    progressing = RunState.transition(HarnessStore.latest_run(), status: :running)

    assert :ok =
             FavnOrchestrator.RunServer.Persistence.persist_run_step(
               progressing,
               :run_started,
               %{}
             )

    assert Map.has_key?(HarnessStore.latest_run().metadata, "recovery_attention")
    assert fixture.run.id == run.id
  end

  @tag store_opts: [held_task_kinds: []]
  test "ordinary writes preserve attention until explicit resume", %{
    fixture: fixture
  } do
    metadata = Map.put(fixture.run.metadata, "recovery_attention", %{"phase" => "prior_failure"})
    run = %{fixture.run | metadata: metadata} |> RunState.with_snapshot_hash()

    Agent.update(
      Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent),
      &%{&1 | run: run}
    )

    fixture = %{fixture | run: run}
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    assert HarnessStore.latest_run().status == :ok
    assert Map.has_key?(HarnessStore.latest_run().metadata, "recovery_attention")
    assert Enum.all?(HarnessStore.commits(), &Map.has_key?(&1.run.metadata, "recovery_attention"))
  end

  test "cancellation while the inspection task is held terminates the worker and the run", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:materialization_finished, :succeeded, "claim-asset"}, 5_000
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)

    send(pid, {:favn_run_cancel_requested, :operator})

    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :shutdown}, 5_000
    assert_receive {:run_transition_committed, :run_cancelled}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000

    assert HarnessStore.latest_run().status == :cancelled
    assert HarnessStore.latest_run().metadata["cancellation_needs_attention"] == true
    refute_receive {:materialization_finished, :failed, _claim_key}, 20
    refute_receive {:runner_task_cancel_requested, _task_id}, 20
    refute_receive {:reconcile_initial, _generation_id}, 20
  end

  @tag store_opts: [commit_failures: %{run_started: :cancel_once}]
  test "a cancellation racing run start leaves durable work for recovery", %{fixture: fixture} do
    Agent.update(
      Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent),
      &%{&1 | run: %{&1.run | status: :pending}}
    )

    context =
      FavnOrchestrator.Persistence.SystemContext.workspace(
        fixture.run.workspace_id,
        :run_recovery
      )

    assert {:ok, _} = FavnOrchestrator.RunManager.recover_candidate(context, fixture.run.id)
    assert_receive {:ownership_released, @fencing_token}, 5_000
    assert HarnessStore.latest_run().metadata[:cancel_requested]
    refute_receive {:run_transition_committed, :run_cancelled}, 20
    refute_receive {:materialization_finished, _, _}, 20
  end

  @tag store_opts: [held_task_kinds: [], commit_failures: %{step_finished: :history_busy}]
  test "a cancellation hint after a queued completion retry cannot discard its accepted result",
       %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:run_transition_held, :step_finished}, 5_000
    send(pid, {:favn_run_cancel_requested, :operator})
    pending = :sys.get_state(pid)
    assert pending.execution_persist_pending.retry.event_type == :step_finished
    assert {:favn_run_cancel_requested, :operator} in pending.deferred_execution_events
    refute_received {:run_transition_committed, :run_cancelled}

    Agent.update(
      Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent),
      &%{&1 | commit_failures: %{}}
    )

    assert_receive {:materialization_finished, :succeeded, "claim-asset"}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    refute_received {:materialization_finished, :failed, _}
  end

  @tag store_opts: [commit_failures: %{step_finished: :cancel_once}]
  test "cancellation conflicting with settlement leaves the completed task and claim for recovery",
       %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    assert HarnessStore.latest_run().metadata[:cancel_requested]
    assert HarnessStore.latest_run().metadata[:active_runner_task_ids] == [@asset_task_id]
    refute_receive {:run_transition_committed, :run_cancelled}, 20
    refute_receive {:materialization_finished, _, _}, 20
  end

  test "ownership loss while the inspection task is held stops the run and kills the worker",
       %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)

    Agent.update(
      Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent),
      &%{&1 | renew_result: :fenced}
    )

    send(:sys.get_state(pid).lease_keeper, :renew)

    assert_receive {:ownership_renewal_rejected, @fencing_token}, 1_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :shutdown}, 5_000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :shutdown}, 5_000
    refute_receive {:reconcile_initial, _generation_id}, 20
  end

  test "a keeper deadline stops a blocked coordinator and its owned helper", %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)
    keeper = :sys.get_state(pid).lease_keeper
    :sys.suspend(pid)
    :sys.replace_state(keeper, &%{&1 | deadline: System.monotonic_time(:millisecond) + 9_000})
    send(keeper, :check)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _}, 6_000
  end

  for deadline <- [:checkout_deadline, :acquisition_deadline] do
    test "#{deadline} closes admission outside a blocked acquisition", %{fixture: fixture} do
      {pid, monitor} = start_run(fixture)
      run = :sys.get_state(pid).run_state

      assert {:ok, {maintenance, reference}} =
               FavnOrchestrator.RunTargetMaintenance.register(run, "pending-acquisition")

      initial = :sys.get_state(maintenance).watches["pending-acquisition"]
      assert initial.acquisition_deadline - initial.checkout_deadline == 17_000
      :sys.suspend(pid)

      :sys.replace_state(maintenance, fn state ->
        put_in(
          state.watches["pending-acquisition"][unquote(deadline)],
          System.monotonic_time(:millisecond) - 1
        )
      end)

      # A late checkout acknowledgement must not grant a new budget.
      send(maintenance, {:acquisition_checked_out, reference})
      assert {:error, _} = FavnOrchestrator.RunTargetMaintenance.permit(run, 1_000)
      send(maintenance, :deadline_check)
      assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
    end
  end

  for deadline <- [:checkout_deadline, :acquisition_deadline] do
    test "late waiting admission cannot erase expired #{deadline}", %{fixture: fixture} do
      {pid, monitor} = start_run(fixture)
      run = :sys.get_state(pid).run_state

      assert {:ok, {maintenance, _}} =
               FavnOrchestrator.RunTargetMaintenance.register(run, "late-waiting")

      :sys.suspend(pid)

      :sys.replace_state(maintenance, fn state ->
        put_in(
          state.watches["late-waiting"][unquote(deadline)],
          System.monotonic_time(:millisecond) - 1
        )
      end)

      FavnOrchestrator.RunTargetMaintenance.admission_result(
        run,
        "late-waiting",
        {:ok, %{status: :waiting}}
      )

      assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
    end
  end

  test "an unavailable attention snapshot requests fenced diagnosis and reports failure", %{
    fixture: fixture
  } do
    agent = Application.fetch_env!(:favn_orchestrator, :post_step_run_server_agent)
    Agent.update(agent, &Map.put(&1, :read_failure, Error.new(:unavailable, "test unavailable")))

    assert {:error, %{kind: :unavailable}} =
             FavnOrchestrator.RunServer.RecoveryAttention.record(
               %{
                 fixture.run
                 | storage_fencing_token: @fencing_token,
                   storage_owner_id: "test-owner"
               },
               :unsafe_recovery
             )

    assert_receive {:diagnosis_required, "attention_snapshot_unavailable", @fencing_token}
  end

  test "manager death removes the complete run subtree before replacement", %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)
    keeper = :sys.get_state(pid).lease_keeper
    keeper_monitor = Process.monitor(keeper)
    manager = Process.whereis(FavnOrchestrator.RunManager)
    Process.exit(manager, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _}, 6_000
    assert_receive {:DOWN, ^keeper_monitor, :process, ^keeper, _}, 6_000
    refute Process.alive?(manager)
  end

  test "late responsiveness response cannot reopen a revoked keeper", %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    keeper = :sys.get_state(pid).lease_keeper
    :sys.suspend(pid)
    state = :sys.get_state(keeper)

    :sys.replace_state(
      keeper,
      &%{&1 | last_response: System.monotonic_time(:millisecond) - 45_001}
    )

    send(keeper, {:lease_response, pid, @fencing_token, state.challenge})
    send(keeper, :check)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
  end

  test "keeper death stops a suspended coordinator without relying on terminate", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)
    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)
    keeper = :sys.get_state(pid).lease_keeper
    :sys.suspend(pid)
    Process.exit(keeper, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 6_000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, _}, 6_000
  end

  @tag store_opts: [commit_failures: %{step_finished: :fenced}]
  test "a fenced step write stops the run process without scheduling a retry", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, :run_ownership_lost}}, 5_000

    step_commits = Enum.filter(HarnessStore.commits(), &(&1.event_type == :step_finished))
    assert length(step_commits) == 1
    refute_receive {:materialization_finished, _status, _claim_key}, 20
    refute_receive {:runner_task_held, _kind, _worker}, 20
  end

  @tag store_opts: [commit_failures: %{run_finished: :fenced}, held_task_kinds: []]
  test "a fenced terminal write stops the run process without scheduling a retry", %{
    fixture: fixture
  } do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:reconcile_initial, "gen-asset"}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, :run_ownership_lost}}, 5_000

    terminal_commits = Enum.filter(HarnessStore.commits(), &(&1.event_type == :run_finished))
    assert length(terminal_commits) == 1
  end

  describe "run server routing" do
    test "replayed lease receipt does not restore elapsed budget" do
      observed = DateTime.utc_now()

      receipt = %Ownership{
        workspace_id: "w",
        run_id: "r",
        owner_id: "o",
        fencing_token: 1,
        expires_at: DateTime.add(observed, 120, :second),
        database_observed_at: observed
      }

      assert FavnOrchestrator.RunLeaseKeeper.deadline(receipt, 100) == 120_100
      replay = %{receipt | database_observed_at: DateTime.add(observed, 20, :second)}
      assert FavnOrchestrator.RunLeaseKeeper.deadline(replay, 20_100) == 120_100
    end

    test "a worker reply is deferred while a persist retry is pending", %{fixture: fixture} do
      ref = make_ref()

      execution_state = %RunExecutionState{
        run: fixture.run,
        post_step_continuations: %{ref => %{pid: self(), pending: %{}}}
      }

      pending = %{
        execution_state: execution_state,
        execution_persist_pending: %{token: make_ref(), retry: nil, reason: :forced}
      }

      assert {:noreply, deferred} = RunServer.handle_info({ref, :ok}, pending)
      assert deferred.deferred_execution_events == [{ref, :ok}]

      unknown = make_ref()
      assert {:noreply, ^pending} = RunServer.handle_info({unknown, :ok}, pending)
    end

    test "continue messages are deferred while execution persistence is pending", %{
      fixture: fixture
    } do
      pending = %{
        execution_state: %RunExecutionState{run: fixture.run},
        execution_persist_pending: %{token: make_ref(), retry: nil, reason: :forced}
      }

      assert {:noreply, deferred} = RunServer.handle_info(:continue_execution, pending)
      assert deferred.deferred_execution_events == [:continue_execution]
    end

    test "a worker reply for a known reference reaches execution", %{fixture: fixture} do
      ref = make_ref()

      execution_state = %RunExecutionState{
        run: fixture.run,
        post_step_continuations: %{ref => %{pid: self(), pending: %{}}}
      }

      assert {:noreply, next} =
               RunServer.handle_info({ref, :ok}, %{execution_state: execution_state})

      assert next.execution_state.post_step_continuations == %{}

      assert {:noreply, ^next} = RunServer.handle_info({ref, :ok}, next)
    end
  end

  defp start_run(fixture) do
    ownership = %Ownership{
      workspace_id: fixture.run.workspace_id,
      run_id: fixture.run.id,
      owner_id: "run-owner",
      fencing_token: @fencing_token,
      expires_at: DateTime.add(DateTime.utc_now(), 120, :second),
      database_observed_at: DateTime.utc_now()
    }

    {:ok, pid} =
      FavnOrchestrator.TestSupport.ManagedRun.start_link(%{
        run_state: fixture.run,
        version: fixture.version,
        recovering?: true,
        storage_ownership: ownership
      })

    Process.unlink(pid)
    {pid, Process.monitor(pid)}
  end

  defp complete_asset_task(fixture) do
    assert_receive {:run_transition_committed, :step_running}, 5_000
    task = running_asset_task(fixture)

    result = %RunnerResult{
      run_id: fixture.run.id,
      manifest_version_id: fixture.run.manifest_version_id,
      manifest_content_hash: fixture.run.manifest_content_hash,
      required_runner_release_id: fixture.release_id,
      status: :ok,
      asset_results: [],
      metadata: RunnerWork.lifecycle_metadata(task.payload)
    }

    completed = %{task | status: :succeeded, result: result, terminal_at: DateTime.utc_now()}
    HarnessStore.put_task(completed)
    RunnerTaskResultRouter.notify(completed)
  end

  defp checkpoint(fixture) do
    {:ok, payload} =
      FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpoint.encode_payload(
        fixture.run.id,
        fixture.freshness_context
      )

    %FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint{
      workspace_id: fixture.run.workspace_id,
      run_id: fixture.run.id,
      owner_id: "run-owner",
      fencing_token: @fencing_token,
      checkpoint_version: 1,
      checkpoint_revision: 1,
      checkpoint_sequence: 3,
      stage: 0,
      attempt: 1,
      payload: payload,
      payload_hash: :crypto.hash(:sha256, payload),
      updated_at: fixture.run.inserted_at
    }
  end

  defp running_asset_task(fixture) do
    %RunnerTask{
      workspace_id: fixture.run.workspace_id,
      task_id: @asset_task_id,
      task_kind: :asset_attempt,
      run_id: fixture.run.id,
      asset_step_id: step_id(fixture.run),
      runner_pool: "default",
      required_runner_release_id: fixture.release_id,
      retry_class: :terminal,
      status: :running,
      payload: %RunnerWork{
        run_id: fixture.run.id,
        manifest_version_id: fixture.run.manifest_version_id,
        manifest_content_hash: fixture.run.manifest_content_hash,
        required_runner_release_id: fixture.release_id,
        asset_step_id: step_id(fixture.run),
        asset_ref: @ref,
        deadline_at: DateTime.add(DateTime.utc_now(), 120, :second),
        attempt: 1,
        stage: 0,
        metadata: %{node_key: @node_key}
      },
      orchestration_context: %{
        kind: :pipeline,
        decision: %{
          decision: :run,
          reason: :upstream_refreshed,
          node_key: @node_key,
          freshness_key: "latest"
        },
        freshness_checkpoint: %{
          version: 1,
          revision: 1,
          sequence: 3,
          stage: 0,
          attempt: 1,
          payload_hash: checkpoint(fixture).payload_hash
        },
        freshness_key: "latest",
        materialization_claim: fixture.claim,
        resource_circuit_permits: []
      },
      assignment_generation: 0,
      inserted_at: DateTime.utc_now()
    }
  end

  defp step_id(run), do: FavnOrchestrator.AssetStepIdentity.asset_step_id(run.id, @node_key, @ref)

  defp fixture do
    version = version()
    {:ok, index} = Index.build_from_version(version)

    plan = %Plan{
      target_refs: [@ref],
      target_node_keys: [@node_key],
      topo_order: [@ref],
      stages: [[@ref]],
      node_stages: [[@node_key]],
      nodes: %{
        @node_key => %{
          ref: @ref,
          node_key: @node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: nil,
          evidence_generation_id: "evidence-asset",
          action: :run,
          retry_policy: Favn.Retry.Policy.default(),
          retry_policy_source: :default
        }
      }
    }

    run =
      RunState.new(
        id: "run-post-step-server",
        workspace_id: "workspace-post-step-server",
        deployment_id: "deployment-post-step-server",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: @ref,
        target_refs: [@ref],
        submit_kind: :pipeline,
        plan: plan,
        metadata: %{active_runner_task_ids: [@asset_task_id]}
      )
      |> Map.put(:event_seq, 4)
      |> Map.put(:status, :running)
      |> RunState.with_snapshot_hash()

    claim = %{
      claim_key: "claim-asset",
      workspace_id: run.workspace_id,
      deployment_id: run.deployment_id,
      expires_at: DateTime.add(DateTime.utc_now(), 60, :second),
      run_id: run.id,
      asset_step_id: step_id(run),
      node_key: @node_key,
      owner_id: "run-owner",
      fencing_token: @fencing_token,
      version: 1,
      status: :claimed,
      target_generation_id: "gen-asset",
      evidence_generation_id: "evidence-asset",
      manifest_version_id: run.manifest_version_id,
      manifest_content_hash: run.manifest_content_hash
    }

    %{
      run: run,
      version: version,
      release_id: version.runner_releases["default"],
      claim: claim,
      freshness_context: %{
        assets_by_ref: index.assets_by_ref,
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

  defp version do
    manifest =
      %Manifest{
        assets: [
          FavnTestSupport.with_target_descriptor(%Asset{
            ref: @ref,
            module: elem(@ref, 0),
            name: elem(@ref, 1),
            type: :sql,
            relation:
              RelationRef.new!(
                connection: :warehouse,
                schema: "analytics",
                name: "monthly_orders"
              ),
            materialization: :table,
            execution_package_hash: String.duplicate("a", 64)
          })
        ]
      }
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-post-step-server")
    version
  end
end
