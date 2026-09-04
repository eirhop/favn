defmodule FavnOrchestrator.RunServer.PostStepRunServerTest do
  @moduledoc """
  Run-server-level proof that post-step reconciliation no longer blocks the run
  process.

  A real `RunServer` resumes one recovered pipeline run whose asset task is
  already running. The harness replaces every store with in-memory fakes behind
  the persistence runtime and gates the runner-task store so the relation
  inspection the reconciler needs stays queued until the test releases it. While
  it is held, the test drives ownership renewals, cancellation, and fenced
  writes as ordinary messages to the run process.
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

    def get_run(_query), do: {:ok, latest_run()}

    def commit_transition(command) do
      event_type = command.event.event_type

      case Agent.get_and_update(agent(), &record_commit(&1, command)) do
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

    def claim_run(command), do: {:ok, ownership(command)}

    def renew_run(command) do
      case Agent.get(agent(), & &1.renew_result) do
        :ok ->
          notify({:ownership_renewed, command.fencing_token})
          {:ok, ownership(command)}

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
        expires_at: DateTime.add(DateTime.utc_now(), 30, :second)
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
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunPostStepSupervisor})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerTaskWaitSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})

    on_exit(fn -> Application.delete_env(:favn_orchestrator, :post_step_run_server_agent) end)

    {:ok, fixture: fixture}
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
      send(pid, :renew_storage_ownership)
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
    {:ok, pid} = RunServer.start_link(%{run_state: fixture.run, version: fixture.version})
    Process.unlink(pid)
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    assert HarnessStore.latest_run().metadata[:cancel_requested]
    refute_receive {:run_transition_committed, :run_cancelled}, 20
    refute_receive {:materialization_finished, _, _}, 20
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

  @tag store_opts: [renew_result: :fenced]
  test "ownership loss while the inspection task is held stops the run and kills the worker",
       %{fixture: fixture} do
    {pid, monitor} = start_run(fixture)
    complete_asset_task(fixture)

    assert_receive {:runner_task_held, :relation_inspection, worker}, 5_000
    worker_monitor = Process.monitor(worker)

    send(pid, :renew_storage_ownership)

    assert_receive {:ownership_renewal_rejected, @fencing_token}, 1_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, :run_ownership_lost}}, 5_000
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :shutdown}, 5_000
    refute_receive {:reconcile_initial, _generation_id}, 20
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
      expires_at: DateTime.add(DateTime.utc_now(), 30, :second)
    }

    {:ok, pid} =
      RunServer.start_link(%{
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
      checkpoint_sequence: fixture.run.event_seq,
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
      asset_step_id: "step-asset",
      runner_pool: "default",
      required_runner_release_id: fixture.release_id,
      retry_class: :terminal,
      status: :running,
      payload: %RunnerWork{
        run_id: fixture.run.id,
        manifest_version_id: fixture.run.manifest_version_id,
        manifest_content_hash: fixture.run.manifest_content_hash,
        required_runner_release_id: fixture.release_id,
        asset_step_id: "step-asset",
        asset_ref: @ref,
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
          sequence: fixture.run.event_seq,
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
      |> Map.put(:event_seq, 3)
      |> Map.put(:status, :running)
      |> RunState.with_snapshot_hash()

    claim = %{
      claim_key: "claim-asset",
      workspace_id: run.workspace_id,
      deployment_id: run.deployment_id,
      expires_at: DateTime.add(DateTime.utc_now(), 60, :second),
      run_id: run.id,
      asset_step_id: "step-asset",
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
