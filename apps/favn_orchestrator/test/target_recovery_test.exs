defmodule FavnOrchestrator.TargetRecoveryTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.TargetGeneration
  alias Favn.TargetGenerationRelation
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Results.InitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetRecovery

  @generation_id "018f47a0-7b0d-4b1a-8d8b-e18a9a987654"
  @ref {__MODULE__.Orders, :asset}

  defmodule Store do
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Results.TargetOperationLock
    alias FavnOrchestrator.Persistence.Results.TargetRecoveryOperation

    def get_initial_candidate(_query), do: {:ok, Process.get(:recovery_candidate)}
    def get_runtime_state(_query), do: {:ok, Process.get(:recovery_runtime)}
    def get_deployment_targets(_query), do: {:ok, Process.get(:recovery_grants)}
    def get_deployment_manifest(_query), do: {:ok, Process.get(:recovery_version)}
    def get_deployment_configuration(_query), do: {:ok, %{}}
    def get_manifest(_query), do: {:ok, Process.get(:recovery_version)}
    def get_manifest_size(_selector), do: {:ok, 1_024}

    def create_intent(command) do
      send(Process.get(:recovery_test_pid), {:create_recovery_intent, command})

      case Process.get({:target_recovery_idempotency, command.idempotency_key}) do
        nil ->
          operation =
            %TargetRecoveryOperation{
              workspace_id: command.workspace_context.workspace_id,
              operation_id: command.operation_id,
              target_id: command.target_id,
              recovery_kind: command.recovery_kind,
              desired_manifest_id: command.desired_manifest_id,
              source_manifest_id: command.source_manifest_id,
              target_generation_id: command.target_generation_id,
              materialization_id: command.materialization_id,
              plan_hash: nil,
              plan_version: 1,
              plan_payload: %{},
              state: :planning,
              phase: :collecting_evidence,
              actor_id: command.actor_id,
              reason: command.reason,
              idempotency_key: command.idempotency_key,
              expected_binding_version: command.expected_binding_version,
              expected_physical_fingerprint: nil,
              evaluated_at: command.evaluated_at,
              version: 1,
              inserted_at: command.occurred_at,
              updated_at: command.occurred_at
            }

          Process.put({:target_recovery, command.operation_id}, operation)

          Process.put(
            {:target_recovery_idempotency, command.idempotency_key},
            command.operation_id
          )

          Process.put(:latest_recovery_intent, operation)
          {:ok, operation}

        operation_id ->
          operation = Process.get({:target_recovery, operation_id})
          {:ok, %{operation | idempotency_replay?: true}}
      end
    end

    def finalize_plan(command) do
      send(Process.get(:recovery_test_pid), {:finalize_recovery_plan, command})
      operation = Process.get({:target_recovery, command.operation_id})

      planned = %{
        operation
        | plan_hash: command.plan_hash,
          plan_payload: command.plan_payload,
          state: :planned,
          phase: :planned,
          expected_physical_fingerprint: command.expected_physical_fingerprint,
          version: operation.version + 1,
          updated_at: command.occurred_at
      }

      Process.put({:target_recovery, command.operation_id}, planned)
      {:ok, planned}
    end

    def fail_recovery(command) do
      send(Process.get(:recovery_test_pid), {:fail_recovery_plan, command})
      operation = Process.get({:target_recovery, command.operation_id})

      failed = %{
        operation
        | state: :failed,
          phase: :terminal,
          terminal_error: command.terminal_error,
          version: operation.version + 1,
          completed_at: command.occurred_at,
          updated_at: command.occurred_at
      }

      Process.put({:target_recovery, command.operation_id}, failed)
      Process.put(:latest_recovery_intent, failed)
      {:ok, failed}
    end

    def get(query) do
      case Process.get({:target_recovery, query.operation_id}) do
        nil -> {:error, Error.new(:not_found, "target recovery not found")}
        operation -> {:ok, operation}
      end
    end

    def acquire_many(command) do
      send(Process.get(:recovery_test_pid), {:acquire_recovery_lock, command})
      now = command.occurred_at

      {:ok,
       [
         %TargetOperationLock{
           workspace_id: command.workspace_context.workspace_id,
           target_id: hd(command.target_ids),
           operation_id: command.operation_id,
           operation_type: command.operation_type,
           fencing_token: 4,
           lease_owner: command.lease_owner,
           lease_expires_at: DateTime.add(now, command.lease_duration_ms, :millisecond),
           version: 1,
           inserted_at: now,
           updated_at: now
         }
       ]}
    end

    def begin_recovery(command) do
      operation = Process.get({:target_recovery, command.operation_id})

      applying = %{
        operation
        | state: :applying,
          phase: :marker_intent,
          recovery_token: command.recovery_token,
          version: operation.version + 1,
          started_at: command.occurred_at,
          updated_at: command.occurred_at
      }

      Process.put({:target_recovery, command.operation_id}, applying)
      {:ok, applying}
    end

    def activate_generation(command) do
      send(Process.get(:recovery_test_pid), {:activate_recovered_generation, command})
      operation = Process.get({:target_recovery, command.operation_id})

      completed = %{
        operation
        | state: :succeeded,
          phase: :terminal,
          compatibility_result: %{
            status: command.compatibility_status,
            reason_code: command.reason_code,
            diff: command.compatibility_diff
          },
          result_marker: command.data_plane_marker,
          version: operation.version + 1,
          completed_at: command.occurred_at,
          updated_at: command.occurred_at
      }

      Process.put({:target_recovery, command.operation_id}, completed)
      {:ok, completed}
    end

    def mark_unknown(command) do
      operation = Process.get({:target_recovery, command.operation_id})

      unknown = %{
        operation
        | state: :outcome_unknown,
          phase: :reconciling,
          unknown_outcome: command.unknown_outcome,
          version: operation.version + 1,
          updated_at: command.occurred_at
      }

      Process.put({:target_recovery, command.operation_id}, unknown)
      {:ok, unknown}
    end

    def release_many(command) do
      send(Process.get(:recovery_test_pid), {:release_recovery_lock, command})
      :ok
    end
  end

  defmodule RunnerExecutor do
    def generation_capabilities(_version, _asset_ref, _opts) do
      {:ok,
       %{
         transactional_ddl: :supported,
         physical_inspection: :supported,
         marker_reconciliation: :supported
       }}
    end

    def inspect_relation(request, _opts) do
      case Process.get(:latest_recovery_intent) do
        %{state: :planning, phase: :collecting_evidence} ->
          send(Process.get(:recovery_test_pid), {:inspect_recovery_relation, request})

          case Process.get(:recovery_inspection_error) do
            nil -> {:ok, Process.get(:recovery_inspection)}
            reason -> {:error, reason}
          end

        _missing_intent ->
          {:error, :target_recovery_intent_not_persisted}
      end
    end

    def generation_marker(_version, asset_ref, opts) do
      send(
        Process.get(:recovery_test_pid),
        {:read_recovery_marker, asset_ref, Keyword.fetch!(opts, :require_relation_instance?)}
      )

      {:ok, Process.get(:recovery_marker)}
    end
  end

  setup do
    previous_executor = Application.get_env(:favn_orchestrator, :test_runner_executor)
    previous_opts = Application.get_env(:favn_orchestrator, :test_runner_executor_opts)
    Application.put_env(:favn_orchestrator, :test_runner_executor, RunnerExecutor)
    Application.put_env(:favn_orchestrator, :test_runner_executor_opts, [])

    stores = %Stores{
      registry: Store,
      runs: Store,
      run_submissions: Store,
      runner_tasks: FavnOrchestrator.TestRunnerTaskStore,
      run_ownership: Store,
      scheduler: Store,
      admission: Store,
      resource_circuits: Store,
      target_generations: Store,
      target_recovery: Store,
      rebuilds: Store,
      target_operation_locks: Store,
      materialization: Store,
      backfills: Store,
      operator_reads: Store,
      logs: Store,
      identity: Store,
      maintenance: Store
    }

    start_supervised!(
      {PersistenceRuntime, %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}}
    )

    Process.put(:recovery_test_pid, self())
    version = version()
    asset = hd(version.manifest.assets)
    target_id = asset.target_descriptor.target_id
    now = ~U[2026-07-28 10:00:00Z]

    binding = %TargetBinding{
      workspace_id: "workspace-recovery",
      target_id: target_id,
      desired_manifest_id: version.manifest_version_id,
      desired_descriptor_hash: asset.target_descriptor.descriptor_hash,
      compatibility_status: :operator_decision,
      reason_code: "unmanaged_physical_relation",
      compatibility_diff: %{},
      version: 2,
      updated_at: now
    }

    generation = %TargetGeneration{
      workspace_id: binding.workspace_id,
      target_id: target_id,
      target_generation_id: @generation_id,
      creating_manifest_id: version.manifest_version_id,
      creating_descriptor_hash: asset.target_descriptor.descriptor_hash,
      logical_relation: Map.from_struct(asset.relation),
      physical_relation: %{
        "connection" => "warehouse",
        "catalog" => nil,
        "schema" => "analytics",
        "name" => "orders"
      },
      status: :building,
      version: 1,
      created_at: now,
      updated_at: now
    }

    Process.put(:recovery_version, version)

    Process.put(:recovery_runtime, %RuntimeState{
      workspace_id: binding.workspace_id,
      deployment_id: "deployment-recovery",
      manifest_version_id: version.manifest_version_id,
      revision: 1
    })

    Process.put(:recovery_grants, [
      %DeploymentTarget{
        target_kind: :asset,
        target_id: target_id,
        selection_source: :explicit,
        customer_visible: true,
        descriptor: %{}
      }
    ])

    Process.put(:recovery_candidate, %InitialTargetRecoveryCandidate{
      binding: binding,
      generation: generation,
      materialization_id: "materialization-recovery"
    })

    Process.put(:recovery_inspection, inspection(version, asset))
    Process.put(:recovery_inspection_error, nil)

    Process.put(:recovery_marker, %GenerationMarker{
      target_id: target_id,
      active_relation: asset.relation,
      active_generation_id: @generation_id,
      activation_operation_id: "initial-materialization",
      activation_token: "initial-materialization-token",
      activated_at: now
    })

    on_exit(fn ->
      restore_env(:test_runner_executor, previous_executor)
      restore_env(:test_runner_executor_opts, previous_opts)
    end)

    context = %WorkspaceContext{
      workspace_id: binding.workspace_id,
      principal_id: "operator",
      roles: [:workspace_admin]
    }

    {:ok, context: context, version: version, asset: asset, target_id: target_id}
  end

  test "plans only from exact persisted and physical evidence", fixture do
    evaluated_at = ~U[2026-07-28 10:01:00Z]

    assert {:ok, plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover interrupted materialization",
               operation_id: "target-recovery-test",
               evaluated_at: evaluated_at,
               occurred_at: evaluated_at
             )

    assert plan.payload.target_generation_id == @generation_id
    assert plan.payload.materialization_id == "materialization-recovery"
    assert plan.payload.physical_relation["connection"] == "warehouse"

    assert plan.payload.physical_relation_instance_id ==
             TargetGenerationRelation.instance_id("initial-materialization-token")

    assert_receive {:create_recovery_intent, intent}
    assert intent.expected_binding_version == 2
    assert_receive {:inspect_recovery_relation, request}
    assert request.relation == fixture.asset.relation
    assert request.sample_limit == 0
    assert_receive {:read_recovery_marker, @ref, true}

    assert_receive {:finalize_recovery_plan, command}
    assert command.expected_version == 1
    assert command.expected_physical_fingerprint == plan.payload.physical_fingerprint
  end

  test "refuses recovery when the current relation no longer matches the generation", fixture do
    candidate = Process.get(:recovery_candidate)

    Process.put(:recovery_candidate, %{
      candidate
      | generation: %{
          candidate.generation
          | physical_relation:
              Map.from_struct(
                RelationRef.new!(
                  connection: :warehouse,
                  schema: "analytics",
                  name: "other_orders"
                )
              )
        }
    })

    assert {:error,
            %{kind: :conflict, details: %{reason_code: "target_recovery_relation_changed"}}} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover interrupted materialization"
             )

    refute_receive {:inspect_recovery_relation, _request}
    refute_receive {:create_recovery_intent, _command}
  end

  test "activates only the exact pre-existing Favn marker", fixture do
    evaluated_at = DateTime.utc_now()

    assert {:ok, plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover interrupted materialization",
               operation_id: "target-recovery-start-test",
               evaluated_at: evaluated_at,
               occurred_at: evaluated_at
             )

    assert {:ok, operation} =
             TargetRecovery.start(
               fixture.context,
               plan.plan_id,
               plan.plan_hash,
               occurred_at: evaluated_at
             )

    assert operation.state == :succeeded
    assert_receive {:acquire_recovery_lock, %{operation_type: :target_recovery}}
    assert_receive {:activate_recovered_generation, activation}
    assert activation.expected_operation_version == 3
    assert activation.fencing_token == 4
    assert activation.expected_marker_operation_id == "initial-materialization"
    assert activation.data_plane_marker.activation_token == "initial-materialization-token"
    assert_receive {:release_recovery_lock, _command}
    assert Process.get({:target_recovery, plan.plan_id}).state == :succeeded

    assert {:ok, replayed} =
             TargetRecovery.start(fixture.context, plan.plan_id, plan.plan_hash)

    assert replayed.state == :succeeded
    assert replayed.idempotency_replay?
    refute_receive {:initialize_recovery_marker, _request}
  end

  test "strictly rechecks relation identity while reconciling an unknown outcome", fixture do
    evaluated_at = DateTime.utc_now()

    assert {:ok, plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "reconcile interrupted recovery",
               operation_id: "target-recovery-reconcile-test",
               evaluated_at: evaluated_at,
               occurred_at: evaluated_at
             )

    assert_receive {:read_recovery_marker, @ref, true}

    operation = Process.get({:target_recovery, plan.plan_id})

    Process.put(
      {:target_recovery, plan.plan_id},
      %{
        operation
        | state: :outcome_unknown,
          phase: :reconciling,
          recovery_token: "persisted-recovery-token",
          version: operation.version + 1
      }
    )

    assert {:ok, reconciled} =
             TargetRecovery.reconcile(fixture.context, plan.plan_id, occurred_at: evaluated_at)

    assert reconciled.state == :succeeded
    assert_receive {:read_recovery_marker, @ref, true}
    assert_receive {:read_recovery_marker, @ref, true}
    assert_receive {:activate_recovered_generation, _command}
    assert_receive {:release_recovery_lock, _command}
  end

  test "refuses an unmarked lookalike relation instead of adopting it", fixture do
    Process.put(:recovery_marker, nil)

    assert {:error,
            %{
              kind: :conflict,
              details: %{reason_code: "target_recovery_marker_missing"}
            }} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover interrupted materialization"
             )

    assert_receive {:read_recovery_marker, @ref, true}
    assert_receive {:create_recovery_intent, _command}
    assert_receive {:fail_recovery_plan, failed}
    assert failed.terminal_error["details"]["reason_code"] == "target_recovery_marker_missing"
    assert Process.get({:target_recovery, failed.operation_id}).state == :failed
    refute_receive {:finalize_recovery_plan, _command}
    refute_receive {:initialize_recovery_marker, _request}
  end

  test "refuses a same-schema replacement when the sidecar marker is stale", fixture do
    inspection = Process.get(:recovery_inspection)
    Process.put(:recovery_inspection, %{inspection | table_metadata: %{}})

    assert {:error,
            %{
              kind: :conflict,
              details: %{reason_code: "target_recovery_relation_identity_missing"}
            }} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover interrupted materialization"
             )

    assert_receive {:inspect_recovery_relation, _request}
    assert_receive {:create_recovery_intent, _command}
    assert_receive {:fail_recovery_plan, failed}

    assert failed.terminal_error["details"]["reason_code"] ==
             "target_recovery_relation_identity_missing"

    assert Process.get({:target_recovery, failed.operation_id}).state == :failed
    refute_receive {:finalize_recovery_plan, _command}
  end

  test "keeps transient runner evidence failures resumable", fixture do
    Process.put(:recovery_inspection_error, :runner_task_timeout)
    operation_id = "target-recovery-transient-runner"

    assert {:error, _reason} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "recover after transient runner failure",
               operation_id: operation_id,
               idempotency_key: operation_id
             )

    assert_receive {:create_recovery_intent, _command}
    refute_receive {:fail_recovery_plan, _command}
    assert Process.get({:target_recovery, operation_id}).state == :planning
  end

  test "terminally fails a resumed planning intent when durable evidence drifts", fixture do
    operation_id = "target-recovery-stale-resume"
    Process.put(:recovery_inspection_error, :runner_task_timeout)

    assert {:error, _reason} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "resume stale planning intent",
               operation_id: operation_id,
               idempotency_key: operation_id
             )

    candidate = Process.get(:recovery_candidate)

    Process.put(:recovery_candidate, %{
      candidate
      | binding: %{candidate.binding | version: candidate.binding.version + 1}
    })

    Process.put(:recovery_inspection_error, nil)

    assert {:error, %{kind: :conflict, details: %{reason_code: "target_recovery_plan_stale"}}} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "resume stale planning intent",
               operation_id: operation_id <> ":ignored",
               idempotency_key: operation_id
             )

    assert_receive {:fail_recovery_plan, failed}
    assert Process.get({:target_recovery, failed.operation_id}).state == :failed
  end

  test "fails planning when succeeded inspection evidence has the wrong release", fixture do
    actual_release_id = "rr_" <> String.duplicate("b", 64)
    inspection = Process.get(:recovery_inspection)

    Process.put(
      :recovery_inspection,
      %{inspection | required_runner_release_id: actual_release_id}
    )

    assert {:error, {:runner_release_mismatch, _expected, ^actual_release_id}} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "reject mismatched runner evidence",
               operation_id: "target-recovery-release-mismatch"
             )

    assert_receive {:fail_recovery_plan, failed}
    assert Process.get({:target_recovery, failed.operation_id}).state == :failed
    refute_receive {:finalize_recovery_plan, _command}
  end

  test "start preserves the public semantic evidence error shape", fixture do
    operation_id = "target-recovery-start-evidence-error"

    assert {:ok, plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "plan before evidence changes",
               operation_id: operation_id
             )

    actual_release_id = "rr_" <> String.duplicate("b", 64)
    inspection = Process.get(:recovery_inspection)

    Process.put(
      :recovery_inspection,
      %{inspection | required_runner_release_id: actual_release_id}
    )

    assert {:error, {:runner_release_mismatch, _expected, ^actual_release_id}} =
             TargetRecovery.start(
               fixture.context,
               operation_id,
               plan.plan_hash
             )

    refute_receive {:acquire_recovery_lock, _command}
    assert Process.get({:target_recovery, operation_id}).state == :planned
  end

  test "reconcile preserves the public semantic evidence error shape", fixture do
    operation_id = "target-recovery-reconcile-evidence-error"

    assert {:ok, _plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "plan before reconciliation evidence changes",
               operation_id: operation_id
             )

    operation = Process.get({:target_recovery, operation_id})

    Process.put(
      {:target_recovery, operation_id},
      %{
        operation
        | state: :outcome_unknown,
          phase: :reconciling,
          recovery_token: "persisted-recovery-token",
          version: operation.version + 1
      }
    )

    inspection = Process.get(:recovery_inspection)
    Process.put(:recovery_inspection, %{inspection | table_metadata: %{}})

    assert {:error,
            %{
              kind: :conflict,
              details: %{reason_code: "target_recovery_relation_identity_missing"}
            }} =
             TargetRecovery.reconcile(fixture.context, operation_id)

    refute_receive {:acquire_recovery_lock, _command}
    assert Process.get({:target_recovery, operation_id}).state == :outcome_unknown
  end

  test "replays an immutable plan without failing it after later binding drift", fixture do
    operation_id = "target-recovery-frozen-plan-replay"

    assert {:ok, plan} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "freeze recovery evidence",
               operation_id: operation_id,
               idempotency_key: operation_id
             )

    candidate = Process.get(:recovery_candidate)

    Process.put(:recovery_candidate, %{
      candidate
      | binding: %{candidate.binding | version: candidate.binding.version + 1}
    })

    assert {:ok, replayed} =
             TargetRecovery.plan(
               fixture.context,
               fixture.target_id,
               "freeze recovery evidence",
               operation_id: operation_id <> ":ignored",
               idempotency_key: operation_id
             )

    assert replayed.plan_hash == plan.plan_hash
    assert replayed.idempotency_replay?
    refute_receive {:fail_recovery_plan, _command}
    assert Process.get({:target_recovery, operation_id}).state == :planned
  end

  defp version do
    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: @ref,
        module: elem(@ref, 0),
        name: elem(@ref, 1),
        type: :sql,
        relation: RelationRef.new!(connection: :warehouse, schema: "analytics", name: "orders"),
        materialization: :table,
        execution_package_hash: String.duplicate("a", 64)
      })

    manifest =
      %Manifest{assets: [asset]}
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} = Version.new(manifest, manifest_version_id: "manifest-recovery")
    version
  end

  defp inspection(version, asset) do
    %RelationInspectionResult{
      asset_ref: asset.ref,
      required_runner_release_id: Map.fetch!(version.runner_releases, "default"),
      relation: %{catalog: nil, schema: "analytics", name: "orders", type: :table},
      columns: [%{name: "id", data_type: "BIGINT", nullable?: false}],
      table_metadata: %{
        relation_instance_id:
          TargetGenerationRelation.instance_id("initial-materialization-token")
      },
      adapter: FavnTestSupport.TargetAdapter,
      inspected_at: ~U[2026-07-28 10:00:00Z]
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
