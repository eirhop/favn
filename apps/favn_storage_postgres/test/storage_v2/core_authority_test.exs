defmodule FavnStoragePostgres.StorageV2.CoreAuthorityTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Manifest
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.Freshness.Policy
  alias Favn.SQL.Template
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias Favn.RelationRef
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.ActivateRecoveredTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.AuthorizeScheduleOccurrenceDispatch
  alias FavnOrchestrator.Persistence.Commands.AcquireTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.Commands.BeginRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildItems
  alias FavnOrchestrator.Persistence.Commands.ClaimRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.CreateRebuildPlan
  alias FavnOrchestrator.Persistence.Commands.CreateTargetRecoveryIntent
  alias FavnOrchestrator.Persistence.Commands.FailTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.FinalizeTargetRecoveryPlan
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Commands.PutRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Commands.PurgePersistence
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Commands.RecoverAdministratorCredential
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.ReleaseTargetOperationLocks
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Commands.EnsureWritableTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunSubmission
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunnerTask
  alias FavnOrchestrator.Persistence.Commands.BeginTargetRecovery
  alias FavnOrchestrator.Persistence.Commands.ReconcileInitialTargetGeneration
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.LogEntry
  alias FavnOrchestrator.Persistence.Commands.PurgeLogs
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.ResetActorCredential
  alias FavnOrchestrator.Persistence.Commands.RenewMaterializationClaim
  alias FavnOrchestrator.Persistence.Commands.RenewRebuildOperationLease
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences
  alias FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation
  alias FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent
  alias FavnOrchestrator.Persistence.Commands.SetScheduleActivation
  alias FavnOrchestrator.Persistence.Commands.SetActorStatus
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.StartRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanAction
  alias FavnOrchestrator.Persistence.Commands.RebuildPlanItem
  alias FavnOrchestrator.Persistence.Commands.RetryRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildCancellation
  alias FavnOrchestrator.Persistence.Commands.RequestRebuildReconciliation
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildAction
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildGeneration
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildItem
  alias FavnOrchestrator.Persistence.Commands.TransitionRebuildOperation
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.RenewExecutionLease
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetOperatorRunOverview
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.GetManifestTargetDescriptors
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetGlobalActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.GetMaterializations
  alias FavnOrchestrator.Persistence.Queries.GetAssetWindowStates
  alias FavnOrchestrator.Persistence.Queries.GetEvidenceBindings
  alias FavnOrchestrator.Persistence.Queries.GetTargetBinding
  alias FavnOrchestrator.Persistence.Queries.GetInitialTargetRecoveryCandidate
  alias FavnOrchestrator.Persistence.Queries.GetTargetRecovery
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.CountExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Queries.PageSessions
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.Queries.PageScheduleOccurrences
  alias FavnOrchestrator.Persistence.Queries.PageSchedules
  alias FavnOrchestrator.Persistence.Queries.PageRebuildItems
  alias FavnOrchestrator.Persistence.Queries.PageRebuildOperations
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.Selectors.ActorByUsername
  alias FavnOrchestrator.Persistence.Selectors.ActorByExternalIdentity
  alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Rebuild.Plan, as: RebuildPlan
  alias FavnOrchestrator.RunCancellation
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.AdminLifecycle
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Operator.Catalogue
  alias FavnOrchestrator.Operator.Audit, as: OperatorAudit
  alias FavnOrchestrator.OperatorContext
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunServer
  alias FavnOrchestrator.RunnerTaskResultRouter
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.Persistence.Commands.ClaimRunnerTask
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunServer.Execution.PipelineRetryCheckpoint
  alias FavnOrchestrator.TargetStatus
  alias FavnOrchestrator.TransitionWriter
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Backend
  alias FavnStoragePostgres.Admission.Store, as: AdmissionStore
  alias FavnStoragePostgres.Backfills.Store, as: BackfillStore
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Materialization.Store, as: MaterializationStore
  alias FavnStoragePostgres.Identity.Store, as: IdentityStore
  alias FavnStoragePostgres.Logs.Store, as: LogStore
  alias FavnStoragePostgres.OperatorReads.Store, as: OperatorReadStore
  alias FavnStoragePostgres.Projections.Projector
  alias FavnStoragePostgres.Maintenance.Store, as: MaintenanceStore
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Rebuilds.Store, as: RebuildStore
  alias FavnStoragePostgres.Release
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimeInputKeyInventory
  alias FavnStoragePostgres.RunOwnership.Store, as: RunOwnershipStore
  alias FavnStoragePostgres.RunSubmissions.Store, as: RunSubmissionStore
  alias FavnStoragePostgres.RunnerTasks.Codec, as: RunnerTaskCodec
  alias FavnStoragePostgres.RunnerTasks.Store, as: RunnerTaskStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.Schemas.ManifestVersion, as: ManifestVersionRow
  alias FavnStoragePostgres.Schemas.RebuildOperation, as: RebuildOperationRow
  alias FavnStoragePostgres.Schemas.RuntimeInputPin, as: RuntimeInputPinRow
  alias FavnStoragePostgres.Scheduler.Store, as: SchedulerStore
  alias FavnStoragePostgres.TargetGenerations.Store, as: TargetGenerationStore
  alias FavnStoragePostgres.TargetOperationLocks.Store, as: TargetOperationLockStore
  alias FavnStoragePostgres.TargetRecoveries.Store, as: TargetRecoveryStore
  alias FavnStoragePostgres.StorageV2.Migrations

  @service_token "B7yN3kQ9wR4mT8xZ2cV6pL1sD5fH0jA7"

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} =
      Config.repo_options(
        url: url,
        ssl_mode: :disable,
        pool: Sandbox,
        pool_size: 4
      )

    start_supervised!({Repo, options})
    start_supervised!({Lifecycle, shutdown_drain_timeout_ms: 120_000})
    :ok = Lifecycle.mark_accepting()
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)

    previous_keys = Application.get_env(:favn_storage_postgres, :runtime_input_pin_keys)

    previous_version =
      Application.get_env(:favn_storage_postgres, :runtime_input_pin_current_key_version)

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_keys, %{
      1 => "0123456789abcdef0123456789abcdef"
    })

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_current_key_version, 1)

    on_exit(fn ->
      restore_env(:runtime_input_pin_keys, previous_keys)
      restore_env(:runtime_input_pin_current_key_version, previous_version)
    end)

    :ok
  end

  setup do
    :ok = Sandbox.checkout(Repo)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: []
      }
    ])

    on_exit(fn -> restore_app_env(:api_service_tokens, previous_tokens) end)

    fixture = provision_deploy_fixture()
    {:ok, fixture}
  end

  test "target operation locks fence takeover and gate new materialization writes", fixture do
    target_ids = [fixture.target_id, fixture.target_id <> ":downstream"]
    occurred_at = DateTime.utc_now()

    acquire = %AcquireTargetOperationLocks{
      workspace_context: fixture.workspace_context,
      command_id: "locks:acquire:" <> fixture.workspace_id,
      target_ids: target_ids,
      operation_id: "rebuild-lock-owner",
      operation_type: :rebuild,
      lease_owner: "rebuild-lock-owner",
      lease_duration_ms: 30_000,
      occurred_at: occurred_at
    }

    assert {:ok, locks} = TargetOperationLockStore.acquire_many(acquire)
    assert Enum.map(locks, & &1.fencing_token) == [1, 1]

    assert {:error, %{kind: :conflict, details: %{reason_code: "target_operation_in_progress"}}} =
             TargetOperationLockStore.acquire_many(%{
               acquire
               | command_id: acquire.command_id <> ":conflict",
                 operation_id: "other-operation",
                 lease_owner: "other-owner"
             })

    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    claim = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "locked-materialization:" <> run.id,
      claim_key: "locked-materialization:" <> run.id,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      evidence_generation_id: evidence_generation_id(fixture),
      partition_key: Favn.Freshness.Key.latest(),
      run_id: run.id,
      owner_id: "materialization-worker",
      lease_duration_ms: 30_000,
      occurred_at: occurred_at
    }

    assert {:error, %{kind: :conflict, details: %{reason_code: "target_operation_in_progress"}}} =
             MaterializationStore.claim(claim)

    assert {:ok, %{status: :claimed}} =
             MaterializationStore.claim(%{
               claim
               | command_id: claim.command_id <> ":rebuild",
                 claim_key: claim.claim_key <> ":rebuild",
                 operation_id: acquire.operation_id
             })

    SQL.query!(
      Repo,
      "UPDATE favn_control.target_operation_locks SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE workspace_id = $1",
      [fixture.workspace_id]
    )

    SQL.query!(
      Repo,
      "UPDATE favn_control.materialization_claims SET expires_at = clock_timestamp() - interval '1 second' WHERE workspace_id = $1",
      [fixture.workspace_id]
    )

    assert {:ok, takeover} =
             TargetOperationLockStore.acquire_many(%{
               acquire
               | command_id: acquire.command_id <> ":takeover",
                 operation_id: "takeover-operation",
                 lease_owner: "takeover-owner"
             })

    assert Enum.map(takeover, & &1.fencing_token) == [2, 2]

    assert :ok =
             TargetOperationLockStore.release_many(%ReleaseTargetOperationLocks{
               workspace_context: fixture.workspace_context,
               command_id: "locks:release:" <> fixture.workspace_id,
               operation_id: "takeover-operation",
               lease_owner: "takeover-owner",
               locks:
                 Enum.map(takeover, &%{target_id: &1.target_id, fencing_token: &1.fencing_token}),
               occurred_at: occurred_at
             })
  end

  test "rebuild plan persistence is immutable, fenced, resumable, and exact-retry safe",
       fixture do
    occurred_at = DateTime.utc_now()
    operation_id = "rebuild-store:" <> fixture.workspace_id
    candidate_id = Ecto.UUID.generate()

    action = %RebuildPlanAction{
      target_id: fixture.target_id,
      ordinal: 0,
      action: :rebuild,
      runner_pool: "default",
      required_runner_release_id: fixture.version.runner_releases["default"],
      reason: %{reason_code: "test_rebuild"},
      upstream_impact: %{},
      pinned_input_generation_ids: [],
      candidate_generation: %{
        target_generation_id: candidate_id,
        descriptor_hash: String.duplicate("a", 64),
        logical_relation: %{
          connection: "warehouse",
          catalog: nil,
          schema: "analytics",
          name: "asset"
        },
        physical_relation: %{
          connection: "warehouse",
          catalog: nil,
          schema: "analytics",
          name: "asset__candidate"
        }
      },
      status: :planned
    }

    item = %RebuildPlanItem{
      target_id: fixture.target_id,
      item_id: "full-load-item",
      ordinal: 0,
      work_kind: :full_load,
      window_key: "full_load",
      window_start: nil,
      window_end: nil,
      candidate_generation_id: candidate_id
    }

    payload = %{
      schema_version: 1,
      operation_id: operation_id,
      manifest_version_id: fixture.version.manifest_version_id,
      target_id: fixture.target_id,
      item_count: 1,
      items_digest: RebuildPlan.hash(%{items: [Map.from_struct(item)]})
    }

    plan_hash = RebuildPlan.hash(payload)

    {:ok, plan_idempotency} =
      CommandIdempotency.new(
        "rebuild.plan",
        :actor,
        fixture.workspace_context.principal_id,
        :crypto.hash(:sha256, operation_id),
        :crypto.hash(:sha256, :erlang.term_to_binary(payload, [:deterministic])),
        DateTime.add(occurred_at, 3_600, :second)
      )

    create = %CreateRebuildPlan{
      workspace_context: fixture.workspace_context,
      command_id: "rebuild:create:" <> fixture.workspace_id,
      operation_id: operation_id,
      root_target_id: fixture.target_id,
      manifest_version_id: fixture.version.manifest_version_id,
      candidate_generation_id: candidate_id,
      plan_hash: plan_hash,
      plan_payload: payload,
      actor_id: fixture.workspace_context.principal_id,
      reason: "test immutable rebuild",
      idempotency_key: operation_id,
      evaluated_at: occurred_at,
      actions: [action],
      items: [item],
      occurred_at: occurred_at,
      idempotency: nil
    }

    planning_payload = %{
      schema_version: 1,
      status: :planning,
      operation_id: operation_id,
      manifest_version_id: fixture.version.manifest_version_id,
      manifest_content_hash: fixture.version.content_hash,
      deployment_id: fixture.deployment_id
    }

    begin = %BeginRebuildPlan{
      workspace_context: fixture.workspace_context,
      command_id: "rebuild:begin:" <> fixture.workspace_id,
      operation_id: operation_id,
      root_target_id: fixture.target_id,
      manifest_version_id: fixture.version.manifest_version_id,
      planning_hash: RebuildPlan.hash(planning_payload),
      planning_payload: planning_payload,
      actor_id: fixture.workspace_context.principal_id,
      reason: "test immutable rebuild",
      idempotency_key: operation_id,
      evaluated_at: occurred_at,
      occurred_at: occurred_at,
      idempotency: plan_idempotency
    }

    assert {:ok, planning} = RebuildStore.begin_plan(begin)
    assert planning.state == :planning
    assert planning.action_count == 0
    assert planning.window_count == 0

    planning_cancel_operation_id = "rebuild-planning-cancel:" <> fixture.workspace_id

    planning_cancel_payload = %{
      planning_payload
      | operation_id: planning_cancel_operation_id
    }

    assert {:ok, %{state: :planning}} =
             RebuildStore.begin_plan(%{
               begin
               | command_id: "rebuild:begin-planning-cancel:" <> fixture.workspace_id,
                 operation_id: planning_cancel_operation_id,
                 planning_hash: RebuildPlan.hash(planning_cancel_payload),
                 planning_payload: planning_cancel_payload,
                 idempotency_key: planning_cancel_operation_id,
                 idempotency: nil
             })

    planning_task_id = "rt_planning_cancel_" <> fixture.workspace_id

    planning_request = %GenerationCapabilitiesRequest{
      manifest: %{fixture.version | manifest: nil},
      asset_ref: {MyApp.Asset, :asset}
    }

    {:ok, planning_task_payload, planning_task_hash} =
      RunnerTaskCodec.encode_payload(:generation_capabilities, planning_request)

    {:ok, planning_task_context} = RunnerTaskCodec.encode_orchestration_context(%{})

    assert {:ok, %{status: :queued}} =
             RunnerTaskStore.enqueue(%EnqueueRunnerTask{
               workspace_context: fixture.workspace_context,
               command_id: "enqueue:planning-cancel:" <> fixture.workspace_id,
               task_id: planning_task_id,
               domain_identity: "planning-cancel:" <> fixture.workspace_id,
               task_kind: :generation_capabilities,
               runner_pool: "default",
               required_runner_release_id: fixture.version.runner_releases["default"],
               retry_class: :safe_to_retry,
               payload: planning_task_payload,
               payload_hash: planning_task_hash,
               orchestration_context: planning_task_context,
               operation_id: planning_cancel_operation_id,
               required_capability: "generation_capabilities",
               issued_at: occurred_at,
               occurred_at: occurred_at
             })

    assert {:ok, planning_cancelled} =
             RebuildStore.request_cancellation(%RequestRebuildCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-planning:" <> fixture.workspace_id,
               operation_id: planning_cancel_operation_id,
               reason: "operator cancelled during planning",
               occurred_at: occurred_at
             })

    assert planning_cancelled.state == :cancelled
    assert planning_cancelled.phase == :terminal
    assert planning_cancelled.candidate_generation_id == nil
    assert planning_cancelled.action_count == 0
    assert planning_cancelled.window_count == 0

    assert {:ok, planning_task} =
             RunnerTaskStore.get(%GetRunnerTask{
               workspace_context: fixture.workspace_context,
               task_id: planning_task_id
             })

    assert planning_task.status == :cancelled
    assert planning_task.cancellation_requested_at == occurred_at
    assert planning_task.terminal_at == occurred_at

    assert {:ok, planned} = RebuildStore.create_plan(create)
    assert planned.state == :planned
    assert planned.plan_hash == plan_hash
    refute planned.idempotency_replay?
    assert planned.actions |> hd() |> Map.fetch!(:progress) == %{planned: 1, total: 1}
    assert {:ok, replayed_plan} = RebuildStore.begin_plan(begin)
    assert replayed_plan.idempotency_replay?
    assert %{replayed_plan | idempotency_replay?: false} == planned
    assert {:ok, ^planned} = RebuildStore.create_plan(create)

    changed_payload = Map.put(payload, :target_id, fixture.target_id <> ":changed")

    assert {:error, %{kind: :conflict}} =
             RebuildStore.create_plan(%{
               create
               | plan_payload: changed_payload,
                 plan_hash: RebuildPlan.hash(changed_payload),
                 idempotency: nil
             })

    assert {:ok, operation_page} =
             RebuildStore.page_operations(%PageRebuildOperations{
               workspace_context: fixture.workspace_context,
               limit: 100
             })

    assert Enum.any?(operation_page.items, &(&1.operation_id == operation_id))

    assert {:ok, item_page} =
             RebuildStore.page_items(%PageRebuildItems{
               workspace_context: fixture.workspace_context,
               operation_id: operation_id,
               limit: 100
             })

    assert [%{item_id: "full-load-item"}] = item_page.items

    assert {:error, %{kind: :not_found}} =
             RebuildStore.page_items(%PageRebuildItems{
               workspace_context: fixture.workspace_context,
               operation_id: "missing-rebuild-operation",
               limit: 100
             })

    assert {:error, %{kind: :not_found}} =
             RebuildStore.page_items(%PageRebuildItems{
               workspace_context: %{
                 fixture.workspace_context
                 | workspace_id: "different-workspace"
               },
               operation_id: operation_id,
               limit: 100
             })

    {:ok, start_idempotency} =
      CommandIdempotency.new(
        "rebuild.start",
        :actor,
        fixture.workspace_context.principal_id,
        :crypto.hash(:sha256, operation_id <> ":start"),
        :crypto.hash(:sha256, plan_hash),
        DateTime.add(occurred_at, 3_600, :second)
      )

    start_command = %StartRebuildOperation{
      workspace_context: fixture.workspace_context,
      command_id: "rebuild:start:" <> fixture.workspace_id,
      operation_id: operation_id,
      plan_hash: plan_hash,
      expected_version: planned.version,
      occurred_at: occurred_at,
      idempotency: start_idempotency
    }

    assert {:ok, queued} =
             RebuildStore.start_operation(start_command)

    assert queued.state == :queued
    assert {:ok, %{state: :queued}} = RebuildStore.start_operation(start_command)

    assert {:ok, claimed_operation} =
             RebuildStore.claim_operation(%ClaimRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:claim-operation:" <> fixture.workspace_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               operation_id: operation_id
             })

    assert claimed_operation.dispatcher.fencing_token == 1

    claimed_row =
      Repo.get_by!(RebuildOperationRow,
        workspace_id: fixture.workspace_id,
        operation_id: operation_id
      )

    assert :ok =
             RebuildStore.renew_operation_lease(%RenewRebuildOperationLease{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:renew-operation:" <> fixture.workspace_id,
               operation_id: operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: claimed_operation.dispatcher.fencing_token,
               lease_duration_ms: 60_000,
               occurred_at: occurred_at
             })

    renewed_row =
      Repo.get_by!(RebuildOperationRow,
        workspace_id: fixture.workspace_id,
        operation_id: operation_id
      )

    assert renewed_row.version == claimed_row.version
    assert DateTime.after?(renewed_row.dispatcher_expires_at, claimed_row.dispatcher_expires_at)

    assert {:error, %{kind: :fenced}} =
             RebuildStore.renew_operation_lease(%RenewRebuildOperationLease{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:renew-operation-stale:" <> fixture.workspace_id,
               operation_id: operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: claimed_operation.dispatcher.fencing_token + 1,
               lease_duration_ms: 60_000,
               occurred_at: occurred_at
             })

    assert {:ok, building} =
             RebuildStore.transition_operation(%TransitionRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:building:" <> fixture.workspace_id,
               operation_id: operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: claimed_operation.dispatcher.fencing_token,
               expected_version: claimed_operation.version,
               expected_states: [:queued],
               state: :building,
               phase: :building,
               occurred_at: occurred_at
             })

    assert building.state == :building

    assert {:ok, [claimed_item]} =
             RebuildStore.claim_items(%ClaimRebuildItems{
               workspace_context: fixture.workspace_context,
               batch_id: "rebuild:claim-item:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               limit: 1
             })

    assert {:ok, claimed_page} =
             RebuildStore.page_items(%PageRebuildItems{
               workspace_context: fixture.workspace_context,
               operation_id: operation_id,
               target_id: fixture.target_id,
               status: :claimed,
               limit: 100
             })

    assert [
             %{
               claim_owner: "rebuild-dispatcher",
               fencing_token: fencing_token,
               claim_expires_at: %DateTime{},
               version: version
             }
           ] = claimed_page.items

    assert fencing_token == claimed_item.fencing_token
    assert version == claimed_item.version

    assert {:error, %{kind: :fenced}} =
             RebuildStore.transition_item(%TransitionRebuildItem{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:item:stale:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               item_id: claimed_item.item_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: claimed_item.fencing_token + 1,
               expected_version: claimed_item.version,
               status: :failed,
               last_error: %{"outcome" => "safe_failure"},
               occurred_at: occurred_at
             })

    assert {:ok, unknown_item} =
             RebuildStore.transition_item(%TransitionRebuildItem{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:item:unknown:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               item_id: claimed_item.item_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: claimed_item.fencing_token,
               expected_version: claimed_item.version,
               status: :outcome_unknown,
               child_run_id: "unknown-child-run",
               last_error: %{"outcome" => "unknown", "reason" => "lost reply"},
               occurred_at: occurred_at
             })

    assert unknown_item.status == :outcome_unknown

    assert {:ok, failed_item} =
             RebuildStore.transition_item(%TransitionRebuildItem{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:item:reconciled-failed:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               item_id: claimed_item.item_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: unknown_item.fencing_token,
               expected_version: unknown_item.version,
               status: :failed,
               last_error: %{"outcome" => "safe_failure", "reason" => "test"},
               occurred_at: occurred_at
             })

    planned_action = hd(building.actions)
    assert planned_action.runner_pool == "default"

    assert planned_action.required_runner_release_id ==
             fixture.version.runner_releases["default"]

    assert {:ok, running_action} =
             RebuildStore.transition_action(%TransitionRebuildAction{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:action:running:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: building.dispatcher.fencing_token,
               expected_version: planned_action.version,
               expected_statuses: [:planned],
               status: :running,
               occurred_at: occurred_at
             })

    assert {:ok, _failed_action} =
             RebuildStore.transition_action(%TransitionRebuildAction{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:action:failed:" <> fixture.workspace_id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: building.dispatcher.fencing_token,
               expected_version: running_action.version,
               expected_statuses: [:running],
               status: :failed,
               terminal_error: %{"outcome" => "safe_failure"},
               occurred_at: occurred_at
             })

    assert failed_item.status == :failed

    assert {:ok, failed} =
             RebuildStore.transition_operation(%TransitionRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:failed:" <> fixture.workspace_id,
               operation_id: operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: building.dispatcher.fencing_token,
               expected_version: building.version,
               expected_states: [:building],
               state: :failed,
               phase: :terminal,
               terminal_error: %{"outcome" => "safe_failure"},
               occurred_at: occurred_at
             })

    assert {:ok, retried} =
             RebuildStore.retry_operation(%RetryRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:retry:" <> fixture.workspace_id,
               operation_id: operation_id,
               plan_hash: plan_hash,
               occurred_at: occurred_at
             })

    assert retried.state == :queued
    assert retried.progress == %{ready: 1, total: 1}
    assert hd(retried.actions).status == :planned
    assert failed.state == :failed

    assert {:ok, recovered_claim} =
             RebuildStore.claim_operation(%ClaimRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:claim-retry:" <> fixture.workspace_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               operation_id: operation_id
             })

    transition = fn current, next_state, phase, label, attrs ->
      RebuildStore.transition_operation(%TransitionRebuildOperation{
        workspace_context: fixture.workspace_context,
        command_id: "rebuild:#{label}:" <> fixture.workspace_id,
        operation_id: operation_id,
        owner_id: "rebuild-dispatcher",
        fencing_token: current.dispatcher.fencing_token,
        expected_version: current.version,
        expected_states: [current.state],
        state: next_state,
        phase: phase,
        cleanup_state: Keyword.get(attrs, :cleanup_state),
        unknown_outcome: Keyword.get(attrs, :unknown_outcome),
        occurred_at: occurred_at
      })
    end

    assert {:ok, retry_building} =
             transition.(recovered_claim, :building, :building, "retry-building", [])

    assert {:ok, retry_validating} =
             transition.(retry_building, :validating, :validating, "retry-validating", [])

    assert {:ok, retry_activating} =
             transition.(retry_validating, :activating, :activating, "retry-activating", [])

    assert {:ok, _activation_unknown} =
             transition.(
               retry_activating,
               :activation_unknown,
               :reconciling,
               "activation-unknown",
               unknown_outcome: %{"outcome" => "unknown"}
             )

    assert {:ok, reconciling} =
             RebuildStore.request_reconciliation(%RequestRebuildReconciliation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:request-reconciliation:" <> fixture.workspace_id,
               operation_id: operation_id,
               occurred_at: occurred_at
             })

    assert reconciling.state == :reconciling

    assert {:ok, repairing} =
             transition.(reconciling, :building, :repairing, "repairing", unknown_outcome: %{})

    assert {:ok, final_validating} =
             transition.(repairing, :validating, :validating, "final-validating", [])

    assert {:ok, final_activating} =
             transition.(final_validating, :activating, :activating, "final-activating", [])

    assert {:ok, succeeded} =
             transition.(final_activating, :succeeded, :terminal, "succeeded",
               cleanup_state: :pending
             )

    assert succeeded.cleanup_state == :pending

    assert {:ok, cleanup_claim} =
             RebuildStore.claim_operation(%ClaimRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:claim-cleanup:" <> fixture.workspace_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               operation_id: operation_id
             })

    assert cleanup_claim.state == :succeeded

    assert {:ok, cleaned} =
             transition.(cleanup_claim, :succeeded, :terminal, "cleanup-complete",
               cleanup_state: :complete
             )

    assert cleaned.cleanup_state == :complete
    assert cleaned.timestamps.completed_at == succeeded.timestamps.completed_at

    cancel_operation_id = "rebuild-cancel-store:" <> fixture.workspace_id
    cancel_candidate_id = Ecto.UUID.generate()

    cancel_action = %{
      action
      | candidate_generation: %{
          action.candidate_generation
          | target_generation_id: cancel_candidate_id
        }
    }

    cancel_item = %{
      item
      | item_id: "cancel-full-load-item",
        candidate_generation_id: cancel_candidate_id
    }

    cancel_payload = %{
      payload
      | operation_id: cancel_operation_id,
        items_digest: RebuildPlan.hash(%{items: [Map.from_struct(cancel_item)]})
    }

    cancel_create = %{
      create
      | command_id: "rebuild:create-cancel:" <> fixture.workspace_id,
        operation_id: cancel_operation_id,
        candidate_generation_id: cancel_candidate_id,
        plan_hash: RebuildPlan.hash(cancel_payload),
        plan_payload: cancel_payload,
        idempotency_key: cancel_operation_id,
        actions: [cancel_action],
        items: [cancel_item],
        idempotency: nil
    }

    cancel_planning_payload = %{
      planning_payload
      | operation_id: cancel_operation_id
    }

    assert {:ok, %{state: :planning}} =
             RebuildStore.begin_plan(%{
               begin
               | command_id: "rebuild:begin-cancel:" <> fixture.workspace_id,
                 operation_id: cancel_operation_id,
                 planning_hash: RebuildPlan.hash(cancel_planning_payload),
                 planning_payload: cancel_planning_payload,
                 idempotency_key: cancel_operation_id,
                 idempotency: nil
             })

    assert {:ok, cancel_planned} =
             RebuildStore.create_plan(cancel_create)

    assert {:ok, cancel_queued} =
             RebuildStore.start_operation(%StartRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:start-cancel:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               plan_hash: RebuildPlan.hash(cancel_payload),
               expected_version: cancel_planned.version,
               occurred_at: occurred_at
             })

    assert {:ok, cancel_claim} =
             RebuildStore.claim_operation(%ClaimRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:claim-cancel:" <> fixture.workspace_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               operation_id: cancel_operation_id
             })

    assert {:ok, cancel_building} =
             RebuildStore.transition_operation(%TransitionRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-building:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: cancel_claim.dispatcher.fencing_token,
               expected_version: cancel_claim.version,
               expected_states: [:queued],
               state: :building,
               phase: :building,
               occurred_at: occurred_at
             })

    cancel_planned_action = hd(cancel_building.actions)

    assert {:ok, cancel_running_action} =
             RebuildStore.transition_action(%TransitionRebuildAction{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-action-running:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: cancel_building.dispatcher.fencing_token,
               expected_version: cancel_planned_action.version,
               expected_statuses: [:planned],
               status: :running,
               occurred_at: occurred_at
             })

    assert {:ok, cancel_unknown_action} =
             RebuildStore.transition_action(%TransitionRebuildAction{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-action-unknown:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: cancel_building.dispatcher.fencing_token,
               expected_version: cancel_running_action.version,
               expected_statuses: [:running],
               status: :outcome_unknown,
               occurred_at: occurred_at
             })

    assert {:ok, cancel_requested} =
             RebuildStore.request_cancellation(%RequestRebuildCancellation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-unknown:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               reason: "operator abandoned plan",
               occurred_at: occurred_at
             })

    assert cancel_requested.state == :cancelling
    assert cancel_requested.cleanup_state == :pending

    assert {:ok, cancelled_action} =
             RebuildStore.transition_action(%TransitionRebuildAction{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-action-reconciled:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               target_id: fixture.target_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: cancel_requested.dispatcher.fencing_token,
               expected_version: cancel_unknown_action.version,
               expected_statuses: [:outcome_unknown],
               status: :cancelled,
               occurred_at: occurred_at
             })

    assert cancelled_action.status == :cancelled

    assert :ok =
             RebuildStore.transition_generation(%TransitionRebuildGeneration{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:discard-cancelled:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               target_id: fixture.target_id,
               candidate_generation_id: cancel_candidate_id,
               owner_id: "rebuild-dispatcher",
               operation_fencing_token: cancel_requested.dispatcher.fencing_token,
               status: :discarded,
               occurred_at: occurred_at
             })

    assert {:ok, cancel_cleanup_failed} =
             RebuildStore.transition_operation(%TransitionRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-cleanup-failed:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: cancel_requested.dispatcher.fencing_token,
               expected_version: cancel_requested.version,
               expected_states: [:cancelling],
               state: :cancelled,
               phase: :terminal,
               cleanup_state: :failed,
               occurred_at: occurred_at
             })

    assert {:ok, cancel_cleanup_claim} =
             RebuildStore.claim_operation(%ClaimRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:claim-cancel-cleanup:" <> fixture.workspace_id,
               owner_id: "rebuild-dispatcher",
               lease_duration_ms: 30_000,
               operation_id: cancel_operation_id
             })

    assert cancel_cleanup_claim.state == :cancelled

    assert {:ok, cancel_cleaned} =
             RebuildStore.transition_operation(%TransitionRebuildOperation{
               workspace_context: fixture.workspace_context,
               command_id: "rebuild:cancel-cleanup-complete:" <> fixture.workspace_id,
               operation_id: cancel_operation_id,
               owner_id: "rebuild-dispatcher",
               fencing_token: cancel_cleanup_claim.dispatcher.fencing_token,
               expected_version: cancel_cleanup_claim.version,
               expected_states: [:cancelled],
               state: :cancelled,
               phase: :terminal,
               cleanup_state: :complete,
               occurred_at: occurred_at
             })

    assert cancel_cleaned.cleanup_state == :complete
    assert cancel_cleanup_failed.cleanup_state == :failed
    assert cancel_queued.state == :queued
    assert cancel_planned.state == :planned

    assert {:ok, first_page} =
             RebuildStore.page_operations(%PageRebuildOperations{
               workspace_context: fixture.workspace_context,
               limit: 1
             })

    assert length(first_page.items) == 1
    assert first_page.has_more?
    assert is_map(first_page.next_cursor)

    assert {:ok, second_page} =
             RebuildStore.page_operations(%PageRebuildOperations{
               workspace_context: fixture.workspace_context,
               after: first_page.next_cursor,
               limit: 1
             })

    assert length(second_page.items) == 1

    assert hd(first_page.items).operation_id != hd(second_page.items).operation_id

    isolated_context = %{fixture.workspace_context | workspace_id: "isolated-workspace"}

    assert {:ok, %{items: []}} =
             RebuildStore.page_operations(%PageRebuildOperations{
               workspace_context: isolated_context,
               limit: 100
             })
  end

  test "workspace provisioning is exact-retry safe", fixture do
    suffix = String.replace_prefix(fixture.workspace_id, "ws-", "")

    command = %ProvisionWorkspace{
      platform_context: fixture.platform_context,
      workspace_id: fixture.workspace_id,
      slug: "customer-" <> suffix,
      display_name: "Customer " <> suffix,
      occurred_at: DateTime.utc_now()
    }

    assert :ok = RegistryStore.provision_workspace(command)

    assert {:error, %{kind: :conflict}} =
             RegistryStore.provision_workspace(%{command | display_name: "Different customer"})

    %{rows: [[1]]} =
      SQL.query!(
        Repo,
        "SELECT count(*) FROM favn_control.outbox_events WHERE workspace_id = $1 AND event_kind = 'workspace.provisioned'",
        [fixture.workspace_id]
      )
  end

  test "reconciles one replay-safe building generation from exact successful evidence",
       fixture do
    descriptor = target_descriptor(fixture)
    occurred_at = DateTime.utc_now()

    command = %EnsureWritableTargetGeneration{
      workspace_context: fixture.workspace_context,
      command_id: "generation:ensure:" <> fixture.workspace_id,
      target_id: fixture.target_id,
      manifest_version_id: fixture.version.manifest_version_id,
      descriptor: descriptor,
      occurred_at: occurred_at
    }

    assert {:ok, first} = TargetGenerationStore.ensure_writable(command)
    assert first.generation.status == :building

    assert first.generation.physical_relation == %{
             "catalog" => nil,
             "connection" => "warehouse",
             "name" => "asset",
             "schema" => "analytics"
           }

    assert first.binding.compatibility_status == :uninitialized
    assert is_nil(first.binding.active_generation_id)

    assert {:ok, replayed} = TargetGenerationStore.ensure_writable(command)
    assert replayed.generation.target_generation_id == first.generation.target_generation_id
    assert replayed.generation.logical_relation == first.generation.logical_relation
    assert replayed.generation.physical_relation == first.generation.physical_relation

    assert {:ok, same_build} =
             TargetGenerationStore.ensure_writable(%{
               command
               | command_id: command.command_id <> ":new-run"
             })

    assert same_build.generation.target_generation_id == first.generation.target_generation_id

    assert {:ok, binding} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert binding == same_build.binding

    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    claim = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "generation:claim:" <> run.id,
      claim_key: "generation:claim:" <> run.id,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      target_generation_id: first.generation.target_generation_id,
      evidence_generation_id: first.generation.target_generation_id,
      partition_key: Favn.Freshness.Key.latest(),
      run_id: run.id,
      owner_id: "generation-worker",
      lease_duration_ms: 30_000,
      occurred_at: occurred_at
    }

    assert {:ok, %{status: :claimed, claim: claimed}} = MaterializationStore.claim(claim)

    assert {:ok, %{status: :materialized}} =
             MaterializationStore.finish(%FinishMaterialization{
               workspace_context: fixture.workspace_context,
               command_id: "generation:finish:" <> run.id,
               claim_key: claim.claim_key,
               owner_id: claim.owner_id,
               fencing_token: claimed.fencing_token,
               expected_version: claimed.version,
               status: :succeeded,
               materialization_id: "generation:materialization:" <> run.id,
               payload: %{"row_count" => 1},
               occurred_at: DateTime.add(occurred_at, 1, :second)
             })

    assert {:ok, after_success} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert after_success.compatibility_status == :uninitialized
    assert is_nil(after_success.active_generation_id)

    assert {:error,
            %{
              kind: :conflict,
              details: %{
                reason_code: "initial_target_generation_reconciliation_required",
                target_id: target_id,
                target_generation_id: target_generation_id
              }
            }} = TargetGenerationStore.ensure_writable(command)

    assert target_id == fixture.target_id
    assert target_generation_id == first.generation.target_generation_id

    fingerprint = String.duplicate("a", 64)

    reconciliation = %ReconcileInitialTargetGeneration{
      workspace_context: fixture.workspace_context,
      command_id: "generation:reconcile:" <> run.id,
      target_id: fixture.target_id,
      manifest_version_id: fixture.version.manifest_version_id,
      target_generation_id: first.generation.target_generation_id,
      materialization_id: "generation:materialization:" <> run.id,
      physical_schema_fingerprint: fingerprint,
      data_plane_marker: %{
        "target_id" => fixture.target_id,
        "active_relation" => %{
          "connection" => "warehouse",
          "catalog" => nil,
          "schema" => "analytics",
          "name" => "orders"
        },
        "active_generation_id" => first.generation.target_generation_id,
        "activation_operation_id" => "initial-materialization-operation",
        "activation_token" => "initial-marker-token",
        "activated_at" => "2026-07-22T10:00:00Z"
      },
      occurred_at: DateTime.add(occurred_at, 2, :second)
    }

    assert {:error, %{kind: :conflict}} =
             TargetGenerationStore.reconcile_initial(%{
               reconciliation
               | materialization_id: reconciliation.materialization_id <> ":missing"
             })

    assert {:error, %{kind: :invalid}} =
             TargetGenerationStore.reconcile_initial(%{
               reconciliation
               | data_plane_marker: %{
                   reconciliation.data_plane_marker
                   | "active_generation_id" => Ecto.UUID.generate()
                 }
             })

    assert {:ok, reconciled} = TargetGenerationStore.reconcile_initial(reconciliation)
    assert reconciled.materialization_id == reconciliation.materialization_id
    assert reconciled.generation.status == :active
    assert reconciled.generation.physical_schema_fingerprint == fingerprint
    assert reconciled.generation.data_plane_marker == reconciliation.data_plane_marker
    assert reconciled.binding.active_generation_id == first.generation.target_generation_id
    assert reconciled.binding.compatibility_status == :ready
    assert reconciled.binding.reason_code == "initial_materialization_reconciled"

    assert {:ok, ^reconciled} = TargetGenerationStore.reconcile_initial(reconciliation)

    assert %{rows: [["active", ^fingerprint]]} =
             SQL.query!(
               Repo,
               "SELECT status, physical_schema_fingerprint FROM favn_control.asset_target_generations WHERE workspace_id = $1 AND target_id = $2 AND target_generation_id::text = $3",
               [fixture.workspace_id, fixture.target_id, first.generation.target_generation_id]
             )

    isolated = provision_deploy_fixture(fixture.version)

    assert {:ok, isolated_generation} =
             TargetGenerationStore.ensure_writable(%{
               command
               | workspace_context: isolated.workspace_context,
                 command_id: command.command_id <> ":isolated"
             })

    refute isolated_generation.generation.target_generation_id ==
             first.generation.target_generation_id

    SQL.query!(
      Repo,
      "UPDATE favn_control.asset_target_bindings SET compatibility_status = 'rebuild_required', reason_code = 'test_block' WHERE workspace_id = $1 AND target_id = $2",
      [fixture.workspace_id, fixture.target_id]
    )

    assert {:error, %{kind: :conflict}} =
             TargetGenerationStore.ensure_writable(%{
               command
               | command_id: command.command_id <> ":blocked"
             })

    assert %{rows: [[lifecycle_constraint]]} =
             SQL.query!(
               Repo,
               "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'rebuild_operations_values_valid'",
               []
             )

    assert lifecycle_constraint =~ "activation_unknown"
    assert lifecycle_constraint =~ "reconciling"
    assert lifecycle_constraint =~ "not_started"
    assert lifecycle_constraint =~ "complete"

    assert %{
             rows: [
               ["rebuild_plan_actions_child_operation_fk"],
               ["rebuild_windows_materialization_fk"]
             ]
           } =
             SQL.query!(
               Repo,
               "SELECT conname FROM pg_constraint WHERE conname IN ('rebuild_plan_actions_child_operation_fk', 'rebuild_windows_materialization_fk') ORDER BY conname",
               []
             )
  end

  test "recovers an interrupted initial generation only with exact fenced evidence", fixture do
    occurred_at = DateTime.utc_now()

    assert {:ok, initial} =
             TargetGenerationStore.ensure_writable(%EnsureWritableTargetGeneration{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:ensure:" <> fixture.workspace_id,
               target_id: fixture.target_id,
               manifest_version_id: fixture.version.manifest_version_id,
               descriptor: target_descriptor(fixture),
               occurred_at: occurred_at
             })

    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    claim = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "recovery:claim:" <> run.id,
      claim_key: "recovery:claim:" <> run.id,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      target_generation_id: initial.generation.target_generation_id,
      evidence_generation_id: initial.generation.target_generation_id,
      partition_key: Favn.Freshness.Key.latest(),
      run_id: run.id,
      owner_id: "recovery-worker",
      lease_duration_ms: 30_000,
      occurred_at: occurred_at
    }

    assert {:ok, %{status: :claimed, claim: claimed}} = MaterializationStore.claim(claim)

    materialization_id = "recovery:materialization:" <> run.id

    assert {:ok, %{status: :materialized}} =
             MaterializationStore.finish(%FinishMaterialization{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:finish:" <> run.id,
               claim_key: claim.claim_key,
               owner_id: claim.owner_id,
               fencing_token: claimed.fencing_token,
               expected_version: claimed.version,
               status: :succeeded,
               materialization_id: materialization_id,
               payload: %{"row_count" => 1},
               occurred_at: DateTime.add(occurred_at, 1, :second)
             })

    SQL.query!(
      Repo,
      "UPDATE favn_control.asset_target_bindings SET compatibility_status = 'operator_decision', reason_code = 'unmanaged_physical_relation' WHERE workspace_id = $1 AND target_id = $2",
      [fixture.workspace_id, fixture.target_id]
    )

    assert {:ok, candidate} =
             TargetRecoveryStore.get_initial_candidate(%GetInitialTargetRecoveryCandidate{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    fingerprint = String.duplicate("a", 64)
    operation_id = "target-recovery:" <> run.id
    plan_hash = String.duplicate("b", 64)

    plan_payload = %{
      "expires_at" => "2099-01-01T00:00:00Z",
      "physical_relation_instance_id" =>
        Favn.TargetGenerationRelation.instance_id("recovery-token:" <> run.id),
      "data_plane_marker" => %{
        "target_id" => fixture.target_id,
        "active_relation" => candidate.generation.physical_relation,
        "active_generation_id" => candidate.generation.target_generation_id,
        "activation_operation_id" => "target-recovery-marker:" <> run.id,
        "activation_token" => "recovery-token:" <> run.id,
        "activated_at" => DateTime.to_iso8601(occurred_at)
      }
    }

    assert {:ok, intent} =
             TargetRecoveryStore.create_intent(%CreateTargetRecoveryIntent{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:plan:" <> run.id,
               operation_id: operation_id,
               target_id: fixture.target_id,
               recovery_kind: :reconcile_initial_generation,
               desired_manifest_id: fixture.version.manifest_version_id,
               source_manifest_id: fixture.version.manifest_version_id,
               target_generation_id: candidate.generation.target_generation_id,
               materialization_id: materialization_id,
               actor_id: "recovery-admin",
               reason: "recover interrupted initial materialization",
               idempotency_key: operation_id,
               expected_binding_version: candidate.binding.version,
               evaluated_at: occurred_at,
               occurred_at: occurred_at
             })

    assert intent.state == :planning

    assert_raise Postgrex.Error, fn ->
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.target_recovery_operations
        SET state = 'applying',
            phase = 'marker_intent',
            recovery_token = 'invalid-without-frozen-plan',
            started_at = $3
        WHERE workspace_id = $1 AND operation_id = $2
        """,
        [fixture.workspace_id, operation_id, occurred_at]
      )
    end

    failed_planning_operation_id = operation_id <> ":planning-failure"

    assert {:ok, failed_intent} =
             TargetRecoveryStore.create_intent(%CreateTargetRecoveryIntent{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:failed-plan:" <> run.id,
               operation_id: failed_planning_operation_id,
               target_id: fixture.target_id,
               recovery_kind: :reconcile_initial_generation,
               desired_manifest_id: fixture.version.manifest_version_id,
               source_manifest_id: fixture.version.manifest_version_id,
               target_generation_id: candidate.generation.target_generation_id,
               materialization_id: materialization_id,
               actor_id: "recovery-admin",
               reason: "record deterministic planning failure",
               idempotency_key: failed_planning_operation_id,
               expected_binding_version: candidate.binding.version,
               evaluated_at: occurred_at,
               occurred_at: occurred_at
             })

    assert {:ok, failed_planning} =
             TargetRecoveryStore.fail_recovery(%FailTargetRecovery{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:fail-plan:" <> run.id,
               operation_id: failed_planning_operation_id,
               expected_version: failed_intent.version,
               terminal_error: %{
                 "kind" => "conflict",
                 "reason_code" => "target_recovery_marker_missing"
               },
               occurred_at: DateTime.add(occurred_at, 1, :second)
             })

    assert failed_planning.state == :failed
    assert is_nil(failed_planning.plan_hash)
    assert is_nil(failed_planning.expected_physical_fingerprint)

    assert {:ok, planned} =
             TargetRecoveryStore.finalize_plan(%FinalizeTargetRecoveryPlan{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:finalize-plan:" <> run.id,
               operation_id: operation_id,
               expected_version: intent.version,
               plan_hash: plan_hash,
               plan_payload: plan_payload,
               expected_physical_fingerprint: fingerprint,
               occurred_at: occurred_at
             })

    assert planned.state == :planned

    assert {:ok, replayed_plan} =
             TargetRecoveryStore.create_intent(%CreateTargetRecoveryIntent{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:plan-replay:" <> run.id,
               operation_id: operation_id <> ":new",
               target_id: fixture.target_id,
               recovery_kind: :reconcile_initial_generation,
               desired_manifest_id: fixture.version.manifest_version_id,
               source_manifest_id: fixture.version.manifest_version_id,
               target_generation_id: candidate.generation.target_generation_id,
               materialization_id: materialization_id,
               actor_id: "recovery-admin",
               reason: "recover interrupted initial materialization",
               idempotency_key: operation_id,
               expected_binding_version: candidate.binding.version + 1,
               evaluated_at: DateTime.add(occurred_at, 1, :second),
               occurred_at: DateTime.add(occurred_at, 1, :second)
             })

    assert replayed_plan.idempotency_replay?
    assert replayed_plan.operation_id == operation_id
    assert replayed_plan.plan_hash == plan_hash

    assert {:ok, [lock]} =
             TargetOperationLockStore.acquire_many(%AcquireTargetOperationLocks{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:lock:" <> run.id,
               target_ids: [fixture.target_id],
               operation_id: operation_id,
               operation_type: :target_recovery,
               lease_owner: operation_id,
               lease_duration_ms: 30_000,
               occurred_at: occurred_at
             })

    recovery_token = "recovery-token:" <> run.id

    assert {:ok, applying} =
             TargetRecoveryStore.begin_recovery(%BeginTargetRecovery{
               workspace_context: fixture.workspace_context,
               command_id: "recovery:begin:" <> run.id,
               operation_id: operation_id,
               plan_hash: plan_hash,
               expected_version: planned.version,
               recovery_token: recovery_token,
               occurred_at: occurred_at
             })

    marker = %{
      "target_id" => fixture.target_id,
      "active_relation" => candidate.generation.physical_relation,
      "active_generation_id" => candidate.generation.target_generation_id,
      "activation_operation_id" => "target-recovery-marker:" <> run.id,
      "activation_token" => recovery_token,
      "activated_at" => DateTime.to_iso8601(occurred_at)
    }

    activate = %ActivateRecoveredTargetGeneration{
      workspace_context: fixture.workspace_context,
      command_id: "recovery:activate:" <> run.id,
      operation_id: operation_id,
      expected_operation_version: applying.version,
      target_id: fixture.target_id,
      target_generation_id: candidate.generation.target_generation_id,
      materialization_id: materialization_id,
      source_manifest_id: fixture.version.manifest_version_id,
      expected_binding_version: candidate.binding.version,
      expected_desired_manifest_id: fixture.version.manifest_version_id,
      expected_desired_descriptor_hash: candidate.binding.desired_descriptor_hash,
      physical_schema_fingerprint: fingerprint,
      expected_marker_operation_id: "target-recovery-marker:" <> run.id,
      data_plane_marker: marker,
      compatibility_status: :ready,
      reason_code: "identical",
      compatibility_diff: %{},
      lease_owner: operation_id,
      fencing_token: lock.fencing_token,
      occurred_at: DateTime.add(occurred_at, 2, :second)
    }

    assert {:error, %{kind: :conflict}} =
             TargetRecoveryStore.activate_generation(%{
               activate
               | fencing_token: lock.fencing_token + 1
             })

    assert {:ok, recovered} = TargetRecoveryStore.activate_generation(activate)
    assert recovered.state == :succeeded
    assert recovered.compatibility_result.status == :ready

    assert {:ok, binding} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert binding.active_generation_id == candidate.generation.target_generation_id
    assert binding.compatibility_status == :ready

    assert {:ok, persisted} =
             TargetRecoveryStore.get(%GetTargetRecovery{
               workspace_context: fixture.workspace_context,
               operation_id: operation_id
             })

    assert persisted.state == :succeeded
    assert persisted.result_marker["activation_token"] == recovery_token
  end

  test "persists frozen compatibility before activation and rolls back stale decisions",
       fixture do
    asset = Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))

    persisted_asset =
      %{
        asset
        | relation: RelationRef.new!(connection: :warehouse, schema: "analytics", name: "asset"),
          materialization: :table,
          semantic_generation_id: nil
      }
      |> FavnTestSupport.with_target_descriptor()

    manifest =
      %{
        fixture.version.manifest
        | assets: [persisted_asset | tl(fixture.version.manifest.assets)]
      }
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract()

    {:ok, compatibility_version} =
      Version.new(manifest,
        manifest_version_id: "mv-compatibility-" <> fixture.deployment_id
      )

    assert {:ok, ^compatibility_version} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: compatibility_version
             })

    descriptor = persisted_asset.target_descriptor
    deployment_id = "compatibility-" <> fixture.deployment_id

    decision = %DeploymentTargetCompatibility{
      target_id: fixture.target_id,
      desired_descriptor_hash: descriptor.descriptor_hash,
      compatibility_status: :uninitialized,
      reason_code: "no_active_generation",
      compatibility_diff: %{"physical_relation" => %{"actual" => nil}},
      expected_binding_version: nil,
      expected_active_generation_id: nil,
      active_physical_fingerprint: nil
    }

    command = %{
      fixture.deploy_command
      | deployment_id: deployment_id,
        manifest_version_id: compatibility_version.manifest_version_id,
        target_compatibilities: [decision],
        occurred_at: DateTime.utc_now()
    }

    assert {:error, %{kind: :invalid}} =
             RegistryStore.deploy_manifest(%{
               command
               | deployment_id: "missing-" <> deployment_id,
                 target_compatibilities: []
             })

    private_target_id = TargetStatus.target_id_for_asset({MyApp.PrivateAsset, :private})

    private_target = %DeploymentTarget{
      target_kind: :asset,
      target_id: private_target_id,
      selection_source: :dependency,
      customer_visible: false,
      descriptor: %{"target_id" => private_target_id, "label" => private_target_id}
    }

    assert {:error, %{kind: :invalid}} =
             RegistryStore.deploy_manifest(%{
               command
               | deployment_id: "extra-" <> deployment_id,
                 targets: command.targets ++ [private_target],
                 target_compatibilities: [
                   decision,
                   %{
                     decision
                     | target_id: private_target_id,
                       desired_descriptor_hash: String.duplicate("f", 64)
                   }
                 ]
             })

    assert {:ok, runtime} = RegistryStore.deploy_manifest(command)
    assert runtime.deployment_id == deployment_id

    assert {:ok, binding} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert binding.desired_manifest_id == compatibility_version.manifest_version_id
    assert binding.desired_descriptor_hash == descriptor.descriptor_hash
    assert binding.compatibility_status == :uninitialized
    assert binding.compatibility_diff == decision.compatibility_diff
    assert binding.version == 1

    reclassified_after_commit = %{
      command
      | target_compatibilities: [%{decision | expected_binding_version: binding.version}]
    }

    assert {:ok, replayed_runtime} = RegistryStore.deploy_manifest(reclassified_after_commit)
    assert replayed_runtime.revision == runtime.revision

    blocked_id = "blocked-" <> deployment_id

    blocked_decision = %{
      decision
      | compatibility_status: :rebuild_required,
        reason_code: "contract_changed",
        compatibility_diff: %{"contract" => %{"changed" => true}},
        expected_binding_version: binding.version
    }

    assert {:ok, blocked_runtime} =
             RegistryStore.deploy_manifest(%{
               command
               | deployment_id: blocked_id,
                 target_compatibilities: [blocked_decision]
             })

    assert blocked_runtime.deployment_id == blocked_id

    assert {:ok, blocked_binding} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert blocked_binding.compatibility_status == :rebuild_required
    assert blocked_binding.reason_code == "contract_changed"
    assert blocked_binding.version == 2

    stale_id = "stale-" <> deployment_id

    stale_decision = %{
      blocked_decision
      | compatibility_status: :operator_decision,
        reason_code: "inspection_changed",
        expected_binding_version: 99
    }

    assert {:error, %{kind: :conflict}} =
             RegistryStore.deploy_manifest(%{
               command
               | deployment_id: stale_id,
                 target_compatibilities: [stale_decision]
             })

    assert {:ok, active_runtime} =
             RegistryStore.get_runtime_state(%GetRuntimeState{
               workspace_context: fixture.workspace_context
             })

    assert active_runtime.deployment_id == blocked_id

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.workspace_deployments WHERE workspace_id = $1 AND deployment_id = $2",
               [fixture.workspace_id, stale_id]
             )

    assert {:ok, unchanged_binding} =
             TargetGenerationStore.get_binding(%GetTargetBinding{
               workspace_context: fixture.workspace_context,
               target_id: fixture.target_id
             })

    assert unchanged_binding == blocked_binding
  end

  test "rejects unbound evidence and projects only the durable generation", fixture do
    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)
    evidence_generation_id = evidence_generation_id(fixture)
    unbound_generation_id = "ag_" <> String.duplicate("b", 64)

    window_key =
      Favn.Window.Key.new!(:day, ~U[2026-07-20 00:00:00Z], "Etc/UTC")
      |> Favn.Freshness.Key.window!()

    claim_key = "generation-window:#{run.id}"

    claim = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "claim:" <> claim_key,
      claim_key: claim_key,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      target_generation_id: nil,
      evidence_generation_id: unbound_generation_id,
      partition_key: window_key,
      run_id: run.id,
      owner_id: "generation-window-worker",
      lease_duration_ms: 30_000,
      occurred_at: DateTime.utc_now()
    }

    assert {:error, %{kind: :conflict, details: %{reason_code: "evidence_binding_mismatch"}}} =
             MaterializationStore.claim(claim)

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.materialization_claims
               WHERE workspace_id = $1 AND claim_key = $2
               """,
               [fixture.workspace_id, claim_key]
             )

    assert {:ok, %{status: :claimed, claim: claimed}} =
             MaterializationStore.claim(%{
               claim
               | command_id: claim.command_id <> ":bound",
                 evidence_generation_id: evidence_generation_id
             })

    materialization_id = "materialization:" <> claim_key

    assert {:ok, %{status: :materialized}} =
             MaterializationStore.finish(%FinishMaterialization{
               workspace_context: fixture.workspace_context,
               command_id: "finish:" <> claim_key,
               claim_key: claim_key,
               owner_id: claimed.owner_id,
               fencing_token: claimed.fencing_token,
               expected_version: claimed.version,
               status: :succeeded,
               materialization_id: materialization_id,
               payload: %{
                 "freshness_version" => evidence_generation_id <> ":v1",
                 "run_id" => run.id,
                 "manifest_version_id" => fixture.version.manifest_version_id,
                 "manifest_content_hash" => fixture.version.content_hash,
                 "target_generation_id" => nil,
                 "evidence_generation_id" => evidence_generation_id
               },
               occurred_at: DateTime.utc_now()
             })

    assert {:ok, _publications} = Sequencer.sequence_batch()
    assert drain_projector("generation-window-projector:" <> run.id) > 0

    assert {:ok, [state]} =
             OperatorReadStore.get_asset_window_states(%GetAssetWindowStates{
               workspace_context: fixture.workspace_context,
               evidence_generation_id: evidence_generation_id,
               target_id: fixture.target_id,
               limit: 10
             })

    assert state.evidence_generation_id == evidence_generation_id
    assert state.materialization_id == materialization_id

    assert {:ok, []} =
             OperatorReadStore.get_asset_window_states(%GetAssetWindowStates{
               workspace_context: fixture.workspace_context,
               evidence_generation_id: unbound_generation_id,
               target_id: fixture.target_id,
               limit: 10
             })
  end

  test "registers and deploys an immutable exact manifest catalog", fixture do
    assert {:ok, runtime} =
             RegistryStore.get_runtime_state(%GetRuntimeState{
               workspace_context: fixture.workspace_context
             })

    assert runtime.workspace_id == fixture.workspace_id
    assert runtime.deployment_id == fixture.deployment_id
    assert runtime.manifest_version_id == fixture.version.manifest_version_id
    assert runtime.runner_releases == fixture.version.runner_releases
    assert runtime.revision == 1

    assert {:ok, targets} =
             RegistryStore.get_deployment_targets(%GetDeploymentTargets{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id
             })

    assert targets == fixture.deploy_command.targets

    assert {:ok, visible_targets} =
             RegistryStore.get_deployment_targets(%GetDeploymentTargets{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               customer_visible_only: true
             })

    assert Enum.all?(visible_targets, & &1.customer_visible)

    assert {:ok, ^runtime} = RegistryStore.deploy_manifest(fixture.deploy_command)

    conflicting = %{
      fixture.deploy_command
      | configuration: %{"secret_store_url" => "https://other.example.test"}
    }

    assert {:error, %{kind: :conflict}} = RegistryStore.deploy_manifest(conflicting)

    [first_target | remaining_targets] = fixture.deploy_command.targets

    assert {:error, %{kind: :invalid}} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | deployment_id: fixture.deployment_id <> "-bad-descriptor",
                 targets: [
                   %{first_target | descriptor: %{"target_id" => "wrong", "label" => "Wrong"}}
                   | remaining_targets
                 ]
             })
  end

  test "retains non-persisted evidence bindings across runner releases", fixture do
    initial_asset =
      Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))

    assert {:ok, [initial_binding]} =
             TargetGenerationStore.get_evidence_bindings(%GetEvidenceBindings{
               workspace_context: fixture.workspace_context,
               target_ids: [fixture.target_id]
             })

    assert initial_binding.evidence_generation_id == initial_asset.semantic_generation_id
    assert initial_binding.initial_manifest_id == fixture.version.manifest_version_id

    alternate_manifest =
      fixture.version.manifest
      |> FavnTestSupport.with_manifest_contract(FavnTestSupport.runner_release_id(:alternate))
      |> FavnTestSupport.with_manifest_graph()

    assert {:ok, alternate_version} =
             Version.new(alternate_manifest,
               manifest_version_id: fixture.version.manifest_version_id <> "-alternate"
             )

    alternate_asset =
      Enum.find(alternate_version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))

    refute alternate_asset.semantic_generation_id == initial_asset.semantic_generation_id

    assert {:ok, ^alternate_version} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: alternate_version
             })

    assert {:ok, _runtime} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | deployment_id: fixture.deployment_id <> "-alternate",
                 manifest_version_id: alternate_version.manifest_version_id,
                 occurred_at: DateTime.utc_now()
             })

    assert {:ok, [retained_binding]} =
             TargetGenerationStore.get_evidence_bindings(%GetEvidenceBindings{
               workspace_context: fixture.workspace_context,
               target_ids: [fixture.target_id]
             })

    assert retained_binding == initial_binding
  end

  test "reactivates deduplicated deployment content after another deployment becomes active",
       fixture do
    replacement_id = fixture.deployment_id <> "-replacement"

    replacement = %{
      fixture.deploy_command
      | deployment_id: replacement_id,
        configuration:
          Map.put(
            fixture.deploy_command.configuration,
            "secret_store_url",
            "https://replacement.vault.example.test"
          ),
        occurred_at: DateTime.add(fixture.deploy_command.occurred_at, 1, :second)
    }

    assert {:ok, replacement_runtime} = RegistryStore.deploy_manifest(replacement)
    assert replacement_runtime.deployment_id == replacement_id
    assert replacement_runtime.revision == 2

    rollback_command_id = fixture.deployment_id <> "-rollback-command"

    rollback = %{
      fixture.deploy_command
      | deployment_id: rollback_command_id,
        occurred_at: DateTime.add(fixture.deploy_command.occurred_at, 2, :second)
    }

    assert {:ok, rollback_runtime} = RegistryStore.deploy_manifest(rollback)
    assert rollback_runtime.deployment_id == fixture.deployment_id
    assert rollback_runtime.manifest_version_id == fixture.version.manifest_version_id
    assert rollback_runtime.revision == 3

    assert {:ok, ^rollback_runtime} =
             RegistryStore.get_runtime_state(%GetRuntimeState{
               workspace_context: fixture.workspace_context
             })

    %{rows: [[0]]} =
      SQL.query!(
        Repo,
        """
        SELECT count(*)
        FROM favn_control.workspace_deployments
        WHERE workspace_id = $1 AND deployment_id = $2
        """,
        [fixture.workspace_id, rollback_command_id]
      )

    %{rows: [[aggregate_id, payload_deployment_id]]} =
      SQL.query!(
        Repo,
        """
        SELECT aggregate_id, payload ->> 'deployment_id'
        FROM favn_control.outbox_events
        WHERE workspace_id = $1 AND command_id = $2
        """,
        [fixture.workspace_id, "workspace.deploy:" <> rollback_command_id]
      )

    assert aggregate_id == fixture.deployment_id
    assert payload_deployment_id == fixture.deployment_id
  end

  test "rollback routes new runs to the old release without changing in-flight run pins",
       fixture do
    release_a = fixture.version.runner_releases["default"]
    release_b = FavnTestSupport.runner_release_id(:alternate)

    manifest_b =
      FavnTestSupport.with_manifest_contract(
        fixture.version.manifest,
        %{"default" => release_b}
      )

    assert {:ok, version_b} =
             Version.new(manifest_b,
               manifest_version_id: "mv-rollback-b-#{System.unique_integer([:positive])}"
             )

    assert {:ok, ^version_b} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: version_b
             })

    deployment_b = fixture.deployment_id <> "-release-b"

    assert {:ok, runtime_b} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | deployment_id: deployment_b,
                 manifest_version_id: version_b.manifest_version_id,
                 occurred_at: DateTime.add(fixture.deploy_command.occurred_at, 1, :second)
             })

    assert runtime_b.runner_releases == %{"default" => release_b}

    fixture_b = %{fixture | deployment_id: deployment_b, version: version_b}

    {run_b_command, run_b} =
      create_run_command(fixture_b, "run-release-b-#{fixture.workspace_id}")

    assert {:ok, _created} = RunStore.create_run(run_b_command)

    assert {:ok, rollback_runtime} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | deployment_id: fixture.deployment_id <> "-rollback-intent",
                 occurred_at: DateTime.add(fixture.deploy_command.occurred_at, 2, :second)
             })

    assert rollback_runtime.manifest_version_id == fixture.version.manifest_version_id
    assert rollback_runtime.runner_releases == %{"default" => release_a}

    {run_a_command, run_a} = create_run_command(fixture, "run-release-a-#{fixture.workspace_id}")
    assert {:ok, _created} = RunStore.create_run(run_a_command)

    assert {:ok, persisted_b} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run_b.id
             })

    assert {:ok, persisted_a} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run_a.id
             })

    assert persisted_b.runner_releases == %{"default" => release_b}
    assert persisted_a.runner_releases == %{"default" => release_a}
  end

  test "persists runner release bindings and exposes them through manifest audit reads",
       fixture do
    row = Repo.get!(ManifestVersionRow, fixture.version.manifest_version_id)

    assert row.runner_releases == fixture.version.runner_releases

    {:ok, platform_context} =
      PlatformContext.new("release-auditor", "release-audit-grant", [:platform_reader])

    assert {:ok, page} =
             OperatorReadStore.page_manifests(%PageManifests{
               platform_context: platform_context,
               limit: 500
             })

    summary =
      Enum.find(page.items, &(&1.manifest_version_id == fixture.version.manifest_version_id))

    assert summary.runner_releases == fixture.version.runner_releases
  end

  test "keeps historical manifest audit rows readable but rejects their activation", fixture do
    manifest_version_id = "legacy-mv-#{System.unique_integer([:positive])}"
    content_hash = :crypto.hash(:sha256, manifest_version_id)

    assert {:ok, _result} =
             SQL.query(
               Repo,
               """
               INSERT INTO favn_control.manifest_versions
                 (manifest_version_id, content_hash, schema_version,
                  runner_contract_version, runner_releases,
                  payload_version, asset_count, pipeline_count, schedule_count,
                  atom_strings, manifest, inserted_at)
               VALUES ($1, $2, 9, 9, $3::jsonb, 1, 0, 0, 0, ARRAY[]::text[],
                       jsonb_build_object('assets', jsonb_build_array(),
                                          'pipelines', jsonb_build_array(),
                                          'schedules', jsonb_build_array()),
                       clock_timestamp())
               """,
               [manifest_version_id, content_hash, fixture.version.runner_releases]
             )

    {:ok, platform_context} =
      PlatformContext.new("legacy-auditor", "legacy-audit-grant", [:platform_reader])

    assert {:ok, page} =
             OperatorReadStore.page_manifests(%PageManifests{
               platform_context: platform_context,
               limit: 500
             })

    summary = Enum.find(page.items, &(&1.manifest_version_id == manifest_version_id))
    assert summary.schema_version == 9
    assert summary.runner_releases == fixture.version.runner_releases

    assert {:error,
            %{
              kind: :invalid,
              details: %{
                reason: :historical_manifest_not_activatable,
                schema_version: 9,
                current_schema_version: 14
              }
            }} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | deployment_id: "legacy-deploy-#{System.unique_integer([:positive])}",
                 manifest_version_id: manifest_version_id,
                 occurred_at: DateTime.utc_now()
             })

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])

    response =
      api_request(
        :post,
        "/api/orchestrator/v1/manifests/#{manifest_version_id}/activate",
        activation_body(),
        fixture: fixture,
        identity: api_identity(fixture, [:admin]),
        idempotency_key: "activate-historical-manifest"
      )

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = JSON.decode!(response.resp_body)
  end

  test "projects bounded target descriptors without activating the full manifest", fixture do
    historical_schema_version = Favn.Manifest.Compatibility.current_schema_version() - 1
    descriptor = target_descriptor(fixture, historical_schema_version)

    descriptor_value =
      descriptor
      |> Map.from_struct()
      |> Serializer.encode_canonical!()
      |> Jason.decode!()

    historical_manifest = %{
      "assets" => [%{"target_descriptor" => descriptor_value}],
      "pipelines" => [],
      "schedules" => []
    }

    {1, nil} =
      Repo.update_all(
        from(manifest in ManifestVersionRow,
          where: manifest.manifest_version_id == ^fixture.version.manifest_version_id
        ),
        set: [
          schema_version: historical_schema_version,
          manifest: historical_manifest
        ]
      )

    {:ok, platform_context} =
      PlatformContext.new("descriptor-auditor", "descriptor-audit-grant", [:platform_reader])

    query = %GetManifestTargetDescriptors{
      platform_context: platform_context,
      manifest_version_id: fixture.version.manifest_version_id,
      target_ids: [fixture.target_id, "asset:Elixir.MyApp.Missing:asset"]
    }

    assert {:ok, [persisted]} = RegistryStore.get_manifest_target_descriptors(query)
    assert persisted == descriptor

    assert {:error, %{details: %{reason: :historical_manifest_not_activatable}}} =
             RegistryStore.get_manifest(
               %FavnOrchestrator.Persistence.Queries.ManifestSelector.ById{
                 manifest_version_id: fixture.version.manifest_version_id
               }
             )

    assert {:error, %{kind: :invalid}} =
             RegistryStore.get_manifest_target_descriptors(%{
               query
               | target_ids: Enum.map(1..501, &"asset:Elixir.MyApp.Bounded#{&1}:asset")
             })
  end

  test "release operations refuse an already-started repo without leaking configuration",
       _fixture do
    database_url = System.fetch_env!("FAVN_DATABASE_URL")

    log =
      capture_log(fn ->
        assert {:error, %{operation: :verify_schema, status: :error, code: :repo_already_started}} =
                 Release.verify_schema()
      end)

    refute log =~ database_url

    if database_userinfo = URI.parse(database_url).userinfo do
      refute log =~ database_userinfo
    end
  end

  test "rejects malformed workspace authority before every sensitive read", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    roleless = %{fixture.workspace_context | roles: []}
    malformed = %{fixture.workspace_context | principal_id: ""}
    other = provision_deploy_fixture(fixture.version)

    assert {:error, %{kind: :forbidden}} =
             RegistryStore.get_runtime_state(%GetRuntimeState{workspace_context: roleless})

    assert {:error, %{kind: :forbidden}} =
             RunStore.get_run(%GetRun{workspace_context: malformed, run_id: run.id})

    asset = Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))

    package_query = %GetExecutionPackage{
      workspace_context: roleless,
      deployment_id: fixture.deployment_id,
      manifest_version_id: fixture.version.manifest_version_id,
      asset_ref: asset.ref,
      content_hash: asset.execution_package_hash
    }

    assert {:error, %{kind: :forbidden}} = RegistryStore.get_execution_package(package_query)

    assert {:error, %{kind: :forbidden}} =
             RunStore.get_runtime_inputs(%GetRuntimeInputs{
               workspace_context: roleless,
               run_id: run.id
             })

    assert {:error, %{kind: :not_found}} =
             RunStore.get_run(%GetRun{
               workspace_context: other.workspace_context,
               run_id: run.id
             })

    assert {:error, %{kind: :not_found}} =
             RegistryStore.get_execution_package(%{
               package_query
               | workspace_context: other.workspace_context
             })

    assert {:error, %{kind: :not_found}} =
             RunStore.get_runtime_inputs(%GetRuntimeInputs{
               workspace_context: other.workspace_context,
               run_id: run.id
             })
  end

  test "registers content-addressed execution packages before compact manifests", fixture do
    ref = {MyApp.PackagedAsset, :asset}
    package = execution_package(ref)
    package_hash = package.content_hash
    version = packaged_manifest_version(ref, package_hash)

    missing_query = %MissingExecutionPackageHashes{
      platform_context: fixture.platform_context,
      hashes: [package_hash, package_hash]
    }

    assert {:ok, [package_hash]} ==
             RegistryStore.missing_execution_package_hashes(missing_query)

    assert {:error,
            %{
              kind: :invalid,
              details: %{
                reason: :missing_execution_packages,
                hashes: [^package_hash]
              }
            }} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: version
             })

    command = %RegisterExecutionPackages{
      platform_context: fixture.platform_context,
      packages: [package]
    }

    assert :ok = RegistryStore.register_execution_packages(command)
    assert :ok = RegistryStore.register_execution_packages(command)
    assert {:ok, []} = RegistryStore.missing_execution_package_hashes(missing_query)

    {:ok, package_hash_bytes} = Base.decode16(package_hash, case: :lower)

    batch_fingerprint =
      :crypto.hash(:sha256, package_hash_bytes)
      |> Base.encode16(case: :lower)

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.auth_platform_audit_entries WHERE action = 'execution_packages.registered' AND subject_id = $1",
               [batch_fingerprint]
             )

    assert {:ok, ^version} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: version
             })

    package_deployment_id = "deploy-package-#{System.unique_integer([:positive])}"

    assert {:ok, _runtime} =
             RegistryStore.deploy_manifest(%DeployManifest{
               platform_context: fixture.platform_context,
               workspace_context: fixture.workspace_context,
               deployment_id: package_deployment_id,
               manifest_version_id: version.manifest_version_id,
               configuration: fixture.deploy_command.configuration,
               targets: [
                 %DeploymentTarget{
                   target_kind: :asset,
                   target_id: TargetStatus.target_id_for_asset(ref),
                   selection_source: :explicit,
                   customer_visible: false,
                   descriptor: %{
                     "target_id" => TargetStatus.target_id_for_asset(ref),
                     "label" => inspect(ref)
                   }
                 }
               ],
               schedules: [],
               capacity_scopes: [],
               occurred_at: DateTime.utc_now()
             })

    assert {:error, %{kind: :not_found}} =
             RegistryStore.get_execution_package(%GetExecutionPackage{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               manifest_version_id: version.manifest_version_id,
               asset_ref: ref,
               content_hash: package.content_hash
             })

    assert {:ok, ^package} =
             RegistryStore.get_execution_package(%GetExecutionPackage{
               workspace_context: fixture.workspace_context,
               deployment_id: package_deployment_id,
               manifest_version_id: version.manifest_version_id,
               asset_ref: ref,
               content_hash: package.content_hash
             })

    other_fixture = provision_deploy_fixture(fixture.version)

    assert {:error, %{kind: :not_found}} =
             RegistryStore.get_execution_package(%GetExecutionPackage{
               workspace_context: other_fixture.workspace_context,
               deployment_id: package_deployment_id,
               manifest_version_id: version.manifest_version_id,
               asset_ref: ref,
               content_hash: package.content_hash
             })

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.manifest_execution_packages WHERE manifest_version_id = $1",
               [version.manifest_version_id]
             )
  end

  @tag :slow
  test "manifest package validation is payload-free and batch-bounded", fixture do
    {version, packages} = packaged_manifest_version(501)

    assert :ok =
             RegistryStore.register_execution_packages(%RegisterExecutionPackages{
               platform_context: fixture.platform_context,
               packages: packages
             })

    {result, queries} =
      capture_repo_queries(fn ->
        RegistryStore.register_manifest(%RegisterManifest{
          platform_context: fixture.platform_context,
          version: version
        })
      end)

    assert {:ok, ^version} = result

    validation_queries =
      Enum.filter(queries, fn query ->
        String.contains?(query, ~s(FROM "favn_control"."execution_packages")) and
          String.contains?(query, "FOR KEY SHARE")
      end)

    assert length(validation_queries) == 2
    refute Enum.any?(validation_queries, &Regex.match?(~r/\bpayload\b/i, &1))
  end

  test "purges only old execution packages that no manifest references", fixture do
    unlinked = execution_package({MyApp.OrphanedPackage, :asset})
    linked = execution_package({MyApp.RetainedPackage, :asset})

    assert :ok =
             RegistryStore.register_execution_packages(%RegisterExecutionPackages{
               platform_context: fixture.platform_context,
               packages: [unlinked, linked]
             })

    linked_version = packaged_manifest_version(linked.asset_ref, linked.content_hash)

    assert {:ok, ^linked_version} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: linked_version
             })

    command = %PurgePersistence{
      platform_context: fixture.platform_context,
      job_id: "purge-execution-packages-#{System.unique_integer([:positive])}",
      target: :execution_packages,
      cutoff: DateTime.add(DateTime.utc_now(), 1, :second),
      limit: 10
    }

    assert {:ok, %{status: :completed, batch_count: 1}} = MaintenanceStore.purge(command)

    assert {:ok, [unlinked_hash]} =
             RegistryStore.missing_execution_package_hashes(%MissingExecutionPackageHashes{
               platform_context: fixture.platform_context,
               hashes: [unlinked.content_hash, linked.content_hash]
             })

    assert unlinked_hash == unlinked.content_hash

    assert {:error, %{kind: :invalid}} =
             MaintenanceStore.purge(%{command | workspace_id: fixture.workspace_id})
  end

  test "rejects an execution-package command above the aggregate byte budget", fixture do
    sql = "SELECT 1 AS id\n-- " <> String.duplicate("x", 1_150_000)

    template =
      Template.compile!(sql,
        file: "test/storage_v2/package_batch_limit.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    unique = System.unique_integer([:positive])

    packages =
      Enum.map(1..10, fn index ->
        ref = {MyApp.LargePackage, String.to_atom("batch_#{unique}_#{index}")}
        {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
        package
      end)

    assert {:error, %{kind: :limit_exceeded}} =
             RegistryStore.register_execution_packages(%RegisterExecutionPackages{
               platform_context: fixture.platform_context,
               packages: packages
             })

    assert {:ok, hashes} =
             RegistryStore.missing_execution_package_hashes(%MissingExecutionPackageHashes{
               platform_context: fixture.platform_context,
               hashes: Enum.map(packages, & &1.content_hash)
             })

    assert length(hashes) == length(packages)
  end

  test "HTTP publication uploads missing packages before the compact manifest index" do
    authorize_platform_service_token()

    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:favn, :orchestrator, :manifest_publication_rejected],
          [:favn, :orchestrator, :manifest_publication_succeeded]
        ],
        fn event, measurements, metadata, pid ->
          send(pid, {:manifest_publication, List.last(event), measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    ref = {MyApp.HTTPPackagedAsset, :asset}
    package = execution_package(ref)
    version = packaged_manifest_version(ref, package.content_hash)

    missing =
      api_request(:post, "/api/orchestrator/v1/execution-packages/missing", %{
        "hashes" => [package.content_hash]
      })

    assert missing.status == 200

    assert %{"data" => %{"missing" => [hash]}} = JSON.decode!(missing.resp_body)
    assert hash == package.content_hash

    rejected = publish_manifest_request(version)
    assert rejected.status == 422

    assert_receive {:manifest_publication, :manifest_publication_rejected, %{count: 1},
                    rejected_metadata}

    assert rejected_metadata.status == :rejected
    assert rejected_metadata.reason == :missing_execution_packages
    assert rejected_metadata.manifest_version_id == version.manifest_version_id

    assert %{
             "error" => %{
               "code" => "missing_execution_packages",
               "details" => %{"hashes" => [^hash]}
             }
           } = JSON.decode!(rejected.resp_body)

    upload =
      api_request(:post, "/api/orchestrator/v1/execution-packages", %{
        "packages" => [canonical_json(package)]
      })

    assert upload.status == 201
    assert %{"data" => %{"stored" => 1}} = JSON.decode!(upload.resp_body)

    published = publish_manifest_request(version)
    assert published.status == 201

    assert_receive {:manifest_publication, :manifest_publication_succeeded, %{count: 1},
                    published_metadata}

    assert published_metadata.status == :published
    assert published_metadata.manifest_version_id == version.manifest_version_id
    assert published_metadata.runner_releases == version.runner_releases
  end

  test "HTTP publication keeps a persisted legacy manifest id canonical by content hash",
       fixture do
    authorize_platform_service_token()

    legacy_version = fixture.version
    assert {:ok, derived_version} = Version.new(legacy_version.manifest)

    assert derived_version.content_hash == legacy_version.content_hash
    assert derived_version.manifest_version_id == "mv_" <> derived_version.content_hash
    refute derived_version.manifest_version_id == legacy_version.manifest_version_id

    response = publish_manifest_request(derived_version)

    assert response.status == 200

    assert %{
             "data" => %{
               "manifest" => %{
                 "manifest_version_id" => canonical_id,
                 "content_hash" => content_hash
               },
               "registration" => %{
                 "status" => "already_published",
                 "manifest_version_id" => requested_id,
                 "canonical_manifest_version_id" => canonical_id
               }
             }
           } = JSON.decode!(response.resp_body)

    assert requested_id == derived_version.manifest_version_id
    assert canonical_id == legacy_version.manifest_version_id
    assert content_hash == legacy_version.content_hash

    assert {:ok, persisted} =
             RegistryStore.get_manifest(
               %FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash{
                 content_hash: legacy_version.content_hash
               }
             )

    assert persisted.manifest_version_id == legacy_version.manifest_version_id
    assert {:ok, ^persisted} = Version.verify(persisted)
  end

  test "HTTP execution-package boundary rejects oversized and non-canonical batches", fixture do
    authorize_platform_service_token()

    too_many =
      api_request(:post, "/api/orchestrator/v1/execution-packages", %{
        "packages" => List.duplicate(%{}, 101)
      })

    assert too_many.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = JSON.decode!(too_many.resp_body)

    package = execution_package({MyApp.InvalidHTTPPackage, :asset})
    invalid = Map.put(canonical_json(package), "unknown", true)

    response =
      api_request(:post, "/api/orchestrator/v1/execution-packages", %{
        "packages" => [invalid]
      })

    assert response.status == 422

    assert {:ok, [missing_hash]} =
             RegistryStore.missing_execution_package_hashes(%MissingExecutionPackageHashes{
               platform_context: fixture.platform_context,
               hashes: [package.content_hash]
             })

    assert missing_hash == package.content_hash
  end

  test "HTTP execution-package discovery advertises effective upload limits" do
    authorize_platform_service_token()

    previous = Application.get_env(:favn_orchestrator, :manifest_publication)
    on_exit(fn -> restore_app_env(:manifest_publication, previous) end)

    Application.put_env(:favn_orchestrator, :manifest_publication,
      compressed_limit_bytes: 4_096,
      decompressed_limit_bytes: 16_384
    )

    response =
      api_request(:post, "/api/orchestrator/v1/execution-packages/missing", %{"hashes" => []})

    assert response.status == 200

    assert %{
             "data" => %{
               "publication_limits" => %{
                 "max_packages" => 100,
                 "compressed_limit_bytes" => 4_096,
                 "decompressed_limit_bytes" => 16_384
               }
             }
           } = JSON.decode!(response.resp_body)
  end

  test "HTTP execution-package routes authenticate before reading compressed input" do
    response =
      Plug.Test.conn(:post, "/api/orchestrator/v1/execution-packages", "not-gzip")
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> Plug.Conn.put_req_header("content-encoding", "gzip")
      |> Router.call(Router.init([]))

    assert response.status == 401
    assert %{"error" => %{"code" => "service_unauthorized"}} = JSON.decode!(response.resp_body)
  end

  test "active target lookup rejects targets hidden from the customer", fixture do
    assert {:ok, _version} =
             Manifests.get_active_target_release(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               :asset,
               fixture.target_id
             )

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.workspace_deployment_targets
      SET customer_visible = false
      WHERE workspace_id = $1 AND deployment_id = $2
        AND target_kind = 'asset' AND target_id = $3
      """,
      [fixture.workspace_id, fixture.deployment_id, fixture.target_id]
    )

    assert {:error, :manifest_or_target_not_active_in_workspace} =
             Manifests.get_active_target_release(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               :asset,
               fixture.target_id
             )
  end

  test "workspace manifest reads expose only customer-visible deployment grants", fixture do
    assert {:ok, %{manifest: manifest, targets: targets}} =
             Manifests.active(fixture.workspace_context)

    assert manifest.manifest_version_id == fixture.version.manifest_version_id

    visible_ids =
      Enum.map(targets.assets ++ targets.pipelines, & &1.target_id)
      |> MapSet.new()

    expected_ids =
      fixture.deploy_command.targets
      |> Enum.filter(& &1.customer_visible)
      |> Enum.map(& &1.target_id)
      |> MapSet.new()

    assert visible_ids == expected_ids
  end

  test "workspace deployment requires independent platform mutation authority", fixture do
    {:ok, platform_reader} =
      PlatformContext.new("read-only-consultant", "reader-grant", [:platform_reader])

    assert {:error, :platform_operator_required} =
             Manifests.deploy(
               platform_reader,
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               %{"common_assets" => "all", "common_pipelines" => "all"}
             )

    assert {:error, %{kind: :forbidden}} =
             RegistryStore.deploy_manifest(%{
               fixture.deploy_command
               | platform_context: platform_reader,
                 deployment_id: "unauthorized-#{System.unique_integer([:positive])}"
             })
  end

  test "pipeline backfill planning persists an exact resumable V2 plan", fixture do
    range = %{
      "kind" => "day",
      "from" => "2026-07-01",
      "to" => "2026-07-03",
      "timezone" => "Europe/Oslo"
    }

    assert {:ok, %{window_count: 3, target_id: target_id, window_selection: window_selection}} =
             Backfills.plan_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range
             )

    assert target_id == fixture.pipeline_target_id
    assert window_selection.intent == :backfill
    assert window_selection.expansion == :none
    assert window_selection.requested_anchors == window_selection.effective_anchors
    assert length(window_selection.effective_anchors) == 3

    root_run_id = "run-backfill-#{System.unique_integer([:positive])}"

    assert {:ok, backfill} =
             Backfills.submit_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range,
               root_run_id: root_run_id
             )

    assert backfill.status == :ready
    assert backfill.expected_window_count == 3
    assert backfill.appended_window_count == 3
    assert backfill.range_start == ~U[2026-06-30 22:00:00.000000Z]
    assert backfill.range_end == ~U[2026-07-03 22:00:00.000000Z]

    assert {:ok, pipeline_root} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: backfill.root_run_id
             })

    assert pipeline_root.metadata.window_selection.intent == :backfill
    assert length(pipeline_root.metadata.window_selection.effective_anchors) == 3

    assert {:ok, page} =
             Backfills.page_windows(fixture.workspace_context, backfill.backfill_id, limit: 2)

    assert length(page.items) == 2
    assert page.has_more?
    assert Enum.all?(page.items, &(&1.status == :ready))

    assert {:ok, replayed} =
             Backfills.submit_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range,
               root_run_id: root_run_id
             )

    assert replayed.backfill_id == backfill.backfill_id
    assert replayed.status == backfill.status
    assert replayed.version == backfill.version
  end

  test "failed pipeline backfill planning marks its committed root as failed", fixture do
    root_run_id = "run-backfill-failed-#{System.unique_integer([:positive])}"

    range = %{
      "kind" => "day",
      "from" => "2026-07-01",
      "to" => "2026-07-01",
      "timezone" => "Etc/UTC"
    }

    assert {:error, %Error{kind: :invalid, message: "backfill plan metadata is invalid"}} =
             Backfills.submit_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range,
               root_run_id: root_run_id,
               metadata: %{"large" => String.duplicate("x", 70_000)}
             )

    assert {:ok, root} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: root_run_id
             })

    assert root.status == :error
    assert root.event_seq == 2
    assert root.result[:status] == "failed"
    assert root.result["type"] == "backfill_submission_failed"
    assert root.error["kind"] == "invalid"
    assert root.error["message"] == "backfill plan metadata is invalid"
  end

  test "asset backfills use the same resumable V2 ledger", fixture do
    range = %{
      "kind" => "day",
      "from" => "2026-07-01",
      "to" => "2026-07-02",
      "timezone" => "Etc/UTC"
    }

    assert {:ok, %{window_count: 2, target_id: target_id, window_selection: window_selection}} =
             Backfills.plan_asset(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.target_id,
               range,
               dependencies: :none
             )

    assert target_id == fixture.target_id
    assert window_selection.intent == :backfill
    assert window_selection.expansion == :none
    assert window_selection.requested_anchors == window_selection.effective_anchors

    assert {:ok, backfill} =
             Backfills.submit_asset(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.target_id,
               range,
               root_run_id: "run-asset-backfill-#{System.unique_integer([:positive])}",
               dependencies: :none,
               refresh: {:force_assets, [{MyApp.Asset, :asset}]}
             )

    assert backfill.target_kind == :asset
    assert backfill.expected_window_count == 2

    assert {:ok,
            %RunState{
              submit_kind: :backfill_asset,
              target_refs: [{MyApp.Asset, :asset}],
              metadata: %{window_selection: asset_selection}
            }} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: backfill.root_run_id
             })

    assert asset_selection.intent == :backfill
    assert length(asset_selection.effective_anchors) == 2

    assert {:ok, page} =
             Backfills.page_windows(fixture.workspace_context, backfill.backfill_id, limit: 10)

    assert length(page.items) == 2
    assert Enum.all?(page.items, &(&1.status == :ready))
  end

  test "asset backfills preserve an exact non-contiguous window selection", fixture do
    anchors = [
      Favn.Window.Anchor.new!(
        :day,
        ~U[2026-07-01 00:00:00Z],
        ~U[2026-07-02 00:00:00Z],
        timezone: "Etc/UTC"
      ),
      Favn.Window.Anchor.new!(
        :day,
        ~U[2026-07-03 00:00:00Z],
        ~U[2026-07-04 00:00:00Z],
        timezone: "Etc/UTC"
      )
    ]

    assert {:ok, %{window_count: 2, window_keys: window_keys}} =
             Backfills.plan_asset_windows(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.target_id,
               anchors,
               dependencies: :none
             )

    assert window_keys == Enum.map(anchors, &Favn.Window.Key.encode(&1.key))

    required_generation = %{
      target_id: fixture.target_id,
      evidence_generation_id: "coverage-generation-v1",
      target_generation_id: nil
    }

    assert {:ok, backfill} =
             Backfills.submit_asset_windows(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.target_id,
               anchors,
               root_run_id: "run-exact-asset-backfill-#{System.unique_integer([:positive])}",
               required_generation: required_generation,
               dependencies: :none
             )

    assert backfill.metadata["required_generation"] == %{
             "target_id" => fixture.target_id,
             "evidence_generation_id" => "coverage-generation-v1",
             "target_generation_id" => nil
           }

    assert {:ok, page} =
             Backfills.page_windows(fixture.workspace_context, backfill.backfill_id, limit: 10)

    assert Enum.map(page.items, & &1.window_key) ==
             Enum.map(anchors, &Favn.Window.Key.encode(&1.key))
  end

  test "reports exact backend capabilities and schema readiness" do
    assert :ok = Backend.stores() |> Stores.validate()

    options = [url: System.fetch_env!("FAVN_DATABASE_URL"), ssl_mode: :disable]

    assert {:ok, %{ready?: true, status: :ready} = readiness} = Backend.readiness(options)
    assert readiness.checks.authentication == %{mode: :password, lifecycle_ready?: true}

    assert {:ok, diagnostics} = Backend.diagnostics(options)

    assert diagnostics.engine.name == :postgresql
    assert diagnostics.engine.version.major == 18
    assert diagnostics.schema.ready?
    assert diagnostics.metadata.runtime_input_keys.configured?
  end

  test "readiness rejects a referenced runtime-input key version that is not retained" do
    SQL.query!(
      Repo,
      "INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at) VALUES (99, clock_timestamp())",
      []
    )

    assert {:ok, readiness} =
             Backend.readiness(
               url: System.fetch_env!("FAVN_DATABASE_URL"),
               ssl_mode: :disable
             )

    refute readiness.ready?
    assert readiness.status == :not_ready

    assert readiness.checks.runtime_input_keys.missing_referenced_versions == [99]
    assert 99 in readiness.checks.runtime_input_keys.referenced_versions
  end

  test "schema diagnostics reject malformed and future schemas" do
    SQL.query!(Repo, "DROP INDEX favn_control.runs_recent_idx", [])

    SQL.query!(
      Repo,
      "CREATE INDEX runs_recent_idx ON favn_control.runs (workspace_id, run_id)",
      []
    )

    SQL.query!(Repo, "DROP INDEX favn_control.maintenance_jobs_queue_idx", [])

    SQL.query!(
      Repo,
      "ALTER TABLE favn_control.idempotency_records DROP CONSTRAINT idempotency_records_payload_bounded",
      []
    )

    SQL.query!(Repo, "ALTER TABLE favn_control.maintenance_jobs DROP COLUMN cursor", [])

    SQL.query!(
      Repo,
      "INSERT INTO favn_control.schema_migrations(version, inserted_at) VALUES ($1, clock_timestamp())",
      [20_260_717_999_999]
    )

    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    refute diagnostics.ready?
    assert diagnostics.status == :incompatible
    assert "maintenance_jobs.cursor" in diagnostics.missing_columns
    assert "maintenance_jobs_queue_idx" in diagnostics.missing_critical_indexes

    assert "idempotency_records_payload_bounded" in diagnostics.missing_critical_constraints

    assert 20_260_717_999_999 in diagnostics.future_migration_versions
    refute diagnostics.definition_fingerprint_matches?
  end

  test "schema diagnostics fingerprint every index in the storage schema" do
    SQL.query!(Repo, "DROP INDEX favn_control.run_events_step_cursor_idx", [])

    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    refute diagnostics.ready?
    assert diagnostics.status == :upgrade_required
    assert diagnostics.missing_critical_indexes == []
    refute diagnostics.definition_fingerprint_matches?
  end

  test "schema diagnostics expose projection lag and reject a blocked projector" do
    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.projection_failures
        (projector_name, shard_id, publication_id, workspace_id, event_kind,
         error_kind, error_detail, attempt_count, inserted_at, updated_at)
      VALUES ('control_plane_v1', 0, 1, 'diagnostic-workspace', 'run.submitted',
              'test_failure', '{}'::jsonb, 1, clock_timestamp(), clock_timestamp())
      """,
      []
    )

    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    refute diagnostics.ready?
    assert diagnostics.projection.blocked?
    assert diagnostics.projection.cursor_present?
  end

  test "production readiness rejects the migrator role as a runtime identity" do
    previous = Application.get_env(:favn_storage_postgres, :enforce_runtime_role)
    Application.put_env(:favn_storage_postgres, :enforce_runtime_role, true)
    on_exit(fn -> restore_env(:enforce_runtime_role, previous) end)

    assert {:ok, diagnostics} = Migrations.diagnostics(Repo)
    refute diagnostics.ready?
    assert diagnostics.runtime_role.enforced?
    refute diagnostics.runtime_role.safe?
  end

  test "rejects secret values and targets outside the manifest", fixture do
    secret_command = %{
      fixture.deploy_command
      | deployment_id: fixture.deployment_id <> "-secret",
        configuration: %{"password" => "must-not-be-persisted"}
    }

    assert {:error, %{kind: :invalid}} = RegistryStore.deploy_manifest(secret_command)

    query_secret_command = %{
      fixture.deploy_command
      | deployment_id: fixture.deployment_id <> "-query-secret",
        configuration: %{"secret_store_url" => "https://vault.example.test/?token=secret"}
    }

    assert {:error, %{kind: :invalid}} = RegistryStore.deploy_manifest(query_secret_command)

    unknown_key_command = %{
      fixture.deploy_command
      | deployment_id: fixture.deployment_id <> "-unknown-key",
        configuration: %{"api_key" => "not-allowed"}
    }

    assert {:error, %{kind: :invalid}} = RegistryStore.deploy_manifest(unknown_key_command)

    target_command = %{
      fixture.deploy_command
      | deployment_id: fixture.deployment_id <> "-target",
        targets: [
          %DeploymentTarget{
            target_kind: :asset,
            target_id: "asset:Unknown.Asset:missing",
            selection_source: :explicit,
            customer_visible: true,
            descriptor: %{
              "target_id" => "asset:Unknown.Asset:missing",
              "label" => "Unknown asset"
            }
          }
        ]
    }

    assert {:error, %{kind: :invalid}} = RegistryStore.deploy_manifest(target_command)
  end

  test "atomically creates, transitions, pages, and sequences a run", fixture do
    {command, run} = create_run_command(fixture)

    assert {:ok, created} = RunStore.create_run(command)
    refute created.replayed?
    assert created.run.id == run.id
    assert created.run.runner_releases == fixture.version.runner_releases
    assert created.event.sequence == 1

    assert %{rows: [[2, persisted_releases]]} =
             SQL.query!(
               Repo,
               "SELECT snapshot_version, snapshot->'runner_releases' FROM favn_control.runs WHERE workspace_id = $1 AND run_id = $2",
               [fixture.workspace_id, run.id]
             )

    assert persisted_releases == fixture.version.runner_releases

    assert {:ok, replayed} = RunStore.create_run(command)
    assert replayed.replayed?
    assert replayed.event_id == created.event_id

    running = RunState.transition(run, status: :running)

    transition = %CommitRunTransition{
      workspace_context: fixture.workspace_context,
      command_id: "transition:" <> run.id <> ":2",
      expected_sequence: 1,
      run: running,
      event: %{
        run_id: run.id,
        sequence: 2,
        event_type: :run_started,
        status: :running,
        occurred_at: DateTime.utc_now()
      }
    }

    assert {:ok, committed} = RunStore.commit_transition(transition)
    assert committed.run.status == :running
    assert committed.event.sequence == 2
    assert {:ok, %{replayed?: true}} = RunStore.commit_transition(transition)

    assert {:ok, page} =
             RunStore.page_runs(%PageRuns{scope: fixture.workspace_context, limit: 1})

    assert [%RunState{id: run_id, status: :running}] = page.items
    assert run_id == run.id

    assert {:ok, event_page} =
             RunStore.page_events(%PageRunEvents{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 1
             })

    assert [first_event] = event_page.items
    assert first_event.sequence == 1
    assert event_page.has_more?
    assert event_page.next_cursor == %{sequence: 1}

    %{rows: [[previous_publication_id]]} =
      SQL.query!(
        Repo,
        "SELECT last_publication_id FROM favn_control.outbox_publication_state WHERE singleton_id = 1",
        []
      )

    other_fixture = provision_deploy_fixture(fixture.version)
    {other_command, other_run} = create_run_command(other_fixture)
    assert {:ok, _other_created} = RunStore.create_run(other_command)

    assert {:ok, publications} = Sequencer.sequence_batch()
    assert length(publications) >= 4

    assert Enum.map(publications, & &1.publication_id) ==
             Enum.to_list(
               (previous_publication_id + 1)..(previous_publication_id + length(publications))
             )

    assert {:ok, sequenced_page} =
             RunStore.page_events(%PageRunEvents{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 10
             })

    assert Enum.all?(sequenced_page.items, &is_integer(&1.global_sequence))

    assert {:ok, published_page} =
             RunStore.page_events(%PagePublishedRunEvents{
               scope: fixture.workspace_context,
               after_publication_id: previous_publication_id,
               limit: 10
             })

    assert Enum.map(published_page.items, & &1.run_id) == [run.id, run.id]

    assert Enum.map(published_page.items, & &1.global_sequence) ==
             Enum.sort(Enum.map(published_page.items, & &1.global_sequence))

    assert {:error, %{kind: :invalid}} =
             RunStore.page_events(%PagePublishedRunEvents{
               scope: fixture.workspace_context,
               after_publication_id: List.last(publications).publication_id + 1,
               limit: 10
             })

    response =
      Plug.Test.conn(:get, "/api/orchestrator/v1/streams/runs")
      |> SSE.stream(fixture.workspace_context, {:global, previous_publication_id})

    assert response.status == 200
    assert response.resp_body =~ run.id
    refute response.resp_body =~ other_run.id
  end

  test "rejects a run whose runner release bindings differ from its deployment manifest",
       fixture do
    {command, run} = create_run_command(fixture)
    alternate = FavnTestSupport.runner_release_id(:alternate)

    assert {:error,
            %{
              kind: :constraint,
              details: %{reason: :run_manifest_runner_releases_mismatch}
            }} =
             RunStore.create_run(%{
               command
               | run: %{run | runner_releases: %{"default" => alternate}}
             })
  end

  test "rejects forged manifest content on run creation and transition", fixture do
    {command, run} = create_run_command(fixture)
    forged_hash = String.duplicate("f", 64)

    assert {:error,
            %{
              kind: :constraint,
              details: %{reason: :run_manifest_content_hash_mismatch}
            }} =
             RunStore.create_run(%{command | run: %{run | manifest_content_hash: forged_hash}})

    assert {:ok, _created} = RunStore.create_run(command)

    forged_transition =
      run
      |> RunState.transition(status: :running)
      |> Map.put(:manifest_content_hash, forged_hash)
      |> RunState.with_snapshot_hash()

    assert {:error, %{kind: :conflict}} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "forged-manifest-transition:" <> run.id,
               expected_sequence: 1,
               run: forged_transition,
               event: %{
                 run_id: run.id,
                 sequence: 2,
                 event_type: :run_started,
                 status: :running,
                 occurred_at: DateTime.utc_now()
               }
             })
  end

  test "submission pins runner release bindings from the active manifest", fixture do
    assert {:ok, submission} =
             SubmissionBuilder.persisted_target(
               fixture.workspace_context,
               :asset,
               {MyApp.Asset, :asset},
               fixture.deployment_id,
               fixture.version.manifest_version_id,
               "runner-release-submission-#{fixture.workspace_id}"
             )

    assert submission.run_state.runner_releases == fixture.version.runner_releases
    assert submission.event_metadata.runner_releases == fixture.version.runner_releases
  end

  test "operator run pages return compact relational summaries", fixture do
    {command, run} = create_run_command(fixture)

    run =
      run
      |> Map.put(:params, %{
        window: %{kind: "day", value: "2026-07-16", timezone: "Etc/UTC"}
      })
      |> Map.put(:metadata, %{request_source: "operator-test"})
      |> RunState.with_snapshot_hash()

    assert {:ok, _created} = RunStore.create_run(%{command | run: run})

    assert {:ok, page} =
             OperatorReadStore.page_target_runs(%PageTargetRuns{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               limit: 10
             })

    assert [summary] = page.items

    assert summary.run_id == run.id
    assert summary.status == :pending
    assert summary.event_sequence == 1
    assert summary.runner_releases == fixture.version.runner_releases
    refute Map.has_key?(summary, :run)
  end

  test "target history returns one representative run per execution group", fixture do
    {root_command, root_run} = create_run_command(fixture)
    {child_command, child_run} = create_run_command(fixture)

    child_run = %{
      child_run
      | root_run_id: root_run.id,
        parent_run_id: root_run.id,
        submit_kind: :backfill_pipeline
    }

    child_command = %{child_command | run: RunState.with_snapshot_hash(child_run)}

    assert {:ok, _created} = RunStore.create_run(root_command)
    assert {:ok, _created} = RunStore.create_run(child_command)

    assert {:ok, page} =
             OperatorReadStore.page_target_runs(%PageTargetRuns{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               limit: 10
             })

    assert [summary] = page.items
    assert summary.run_id == child_run.id
    assert summary.root_run_id == root_run.id

    {other_command, other_run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(other_command)

    assert {:ok, first_page} =
             OperatorReadStore.page_target_runs(%PageTargetRuns{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               limit: 1
             })

    assert first_page.has_more?
    assert [%{root_run_id: first_root}] = first_page.items

    {late_child_command, late_child_run} = create_run_command(fixture)

    late_child_run = %{
      late_child_run
      | root_run_id: root_run.id,
        parent_run_id: root_run.id,
        submit_kind: :backfill_pipeline
    }

    late_child_command =
      %{late_child_command | run: RunState.with_snapshot_hash(late_child_run)}

    assert {:ok, _created} = RunStore.create_run(late_child_command)

    assert {:ok, second_page} =
             OperatorReadStore.page_target_runs(%PageTargetRuns{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               after: first_page.next_cursor,
               limit: 1
             })

    assert [%{root_run_id: second_root, run_id: second_representative}] = second_page.items
    assert MapSet.new([first_root, second_root]) == MapSet.new([root_run.id, other_run.id])
    assert second_representative == late_child_run.id
  end

  test "operator overview exposes immutable planned nodes before attempts exist", fixture do
    {command, run} = pipeline_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    assert {:ok, publications} = Sequencer.sequence_batch()
    assert drain_projector("planned-overview:" <> run.id) >= length(publications)

    assert {:ok, overview} =
             OperatorReadStore.get_operator_run_overview(%GetOperatorRunOverview{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 10
             })

    assert [%{asset_ref: "Elixir.MyApp.Asset:asset", stage: 0}] = overview.planned_steps
    assert overview.attempts == []
    refute overview.planned_steps_truncated?
  end

  test "asset detail names what it reads and what reads it", fixture do
    ref = {MyApp.Asset, :asset}
    private_ref = {MyApp.PrivateAsset, :private}

    # MyApp.Asset reads MyApp.PrivateAsset. Nothing reads MyApp.Asset, so the two
    # directions must not come back the same.
    manifest =
      fixture.version.manifest
      |> Map.update!(:assets, fn assets ->
        Enum.map(assets, fn
          %{ref: ^ref} = asset -> %{asset | depends_on: [private_ref]}
          asset -> asset
        end)
      end)
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    {:ok, version} =
      Version.new(manifest, manifest_version_id: "mv-deps-" <> fixture.deployment_id)

    private_target_id = TargetStatus.target_id_for_asset(private_ref)

    deps =
      provision_deploy_fixture(version, [
        %DeploymentTarget{
          target_kind: :asset,
          target_id: private_target_id,
          selection_source: :dependency,
          customer_visible: true,
          descriptor: %{"target_id" => private_target_id, "label" => private_target_id}
        }
      ])

    now = DateTime.utc_now()

    assert {:ok, detail} =
             Catalogue.active_asset_detail(deps.workspace_context, deps.target_id, now: now)

    assert [%{asset_ref: "Elixir.MyApp.PrivateAsset:private", target_id: ^private_target_id}] =
             detail.upstream

    assert detail.downstream == []

    # The reverse edge is recorded nowhere, so it is found by asking every other
    # asset who it reads. That is the half most likely to come back empty.
    assert {:ok, upstream_detail} =
             Catalogue.active_asset_detail(deps.workspace_context, private_target_id, now: now)

    assert upstream_detail.upstream == []
    assert [%{asset_ref: "Elixir.MyApp.Asset:asset"}] = upstream_detail.downstream
  end

  test "asset detail lists its own runs and reads one run's recorded result", fixture do
    {command, run} = create_run_command(fixture)

    run =
      run
      |> Map.put(:status, :ok)
      |> Map.put(:result, %{
        asset_results: [
          %{
            ref: {MyApp.Asset, :asset},
            status: :ok,
            duration_ms: 1_250,
            attempt_count: 1,
            meta: %{rows_written: 4_096, quality_status: :passed}
          }
        ]
      })
      |> RunState.with_snapshot_hash()

    assert {:ok, _created} = RunStore.create_run(%{command | run: run})

    assert {:ok, detail} =
             Catalogue.active_asset_detail(fixture.workspace_context, fixture.target_id,
               now: DateTime.utc_now()
             )

    # A compact history row carries no asset ref, so the ref filter this replaced
    # matched nothing and the asset reported no runs however many it had.
    assert [%{id: run_id, status: :ok}] = detail.runs
    assert run_id == run.id

    assert {:ok, run_detail} =
             Catalogue.active_asset_run_detail(
               fixture.workspace_context,
               fixture.target_id,
               run.id
             )

    assert run_detail.run_id == run.id
    assert run_detail.target_id == fixture.target_id

    # Proves the snapshot is read rather than the compact row, which carries no
    # result at all and would leave every recorded value nil. Meta comes back with
    # string keys, which is why it is handed to the UI whole rather than picked at.
    assert run_detail.asset_result.meta == %{
             "rows_written" => 4_096,
             "quality_status" => "passed"
           }

    assert run_detail.asset_result.duration_ms == 1_250
  end

  test "asset run detail rejects a run belonging to another asset", fixture do
    other_ref = {MyApp.PrivateAsset, :private}
    other_target_id = TargetStatus.target_id_for_asset(other_ref)

    other =
      provision_deploy_fixture(fixture.version, [
        %DeploymentTarget{
          target_kind: :asset,
          target_id: other_target_id,
          selection_source: :explicit,
          customer_visible: true,
          descriptor: %{"target_id" => other_target_id, "label" => other_target_id}
        }
      ])

    {command, run} = create_run_command(other)

    command = %{
      command
      | targets: [
          %RunTarget{
            target_kind: :asset,
            target_id: other_target_id,
            target_module: "MyApp.PrivateAsset",
            target_name: "private",
            is_primary: true
          }
        ],
        run:
          run
          |> Map.put(:asset_ref, other_ref)
          |> Map.put(:target_refs, [other_ref])
          |> RunState.with_snapshot_hash()
    }

    assert {:ok, _created} = RunStore.create_run(command)

    # A run id is a URL parameter, so one asset's page must not render another
    # asset's result just because both runs live in the same workspace.
    assert {:error, :not_found} =
             Catalogue.active_asset_run_detail(
               other.workspace_context,
               other.target_id,
               run.id
             )
  end

  test "run history pages never select or decode authoritative snapshots", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET snapshot = jsonb_build_object('garbage', repeat('x', 3000000)) WHERE workspace_id = $1 AND run_id = $2",
      [fixture.workspace_id, run.id]
    )

    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn_storage_postgres, :repo, :query],
        fn _event, _measurements, metadata, pid ->
          send(pid, {:run_page_query, metadata.query})
        end,
        self()
      )

    try do
      assert {:ok, %{items: [%{run_id: run_id}]} = page} =
               RunStore.page_run_summaries(%PageRuns{
                 scope: fixture.workspace_context,
                 limit: 1
               })

      assert run_id == run.id

      assert [%{target_label: "MyApp.Asset:asset", target_refs: ["MyApp.Asset:asset"]}] =
               page.items

      queries = collect_run_page_queries([])
      assert queries != []
      refute Enum.any?(queries, &Regex.match?(~r/\bsnapshot\b/i, &1))
    after
      :telemetry.detach(handler_id)
    end
  end

  test "compact run history prefers the persisted pipeline target label", fixture do
    {command, run} = pipeline_run_command(fixture)

    pipeline_target = %RunTarget{
      target_kind: :pipeline,
      target_id: fixture.pipeline_target_id,
      target_module: "MyApp.Pipeline",
      target_name: "daily",
      is_primary: false
    }

    assert {:ok, _created} =
             RunStore.create_run(%{command | targets: command.targets ++ [pipeline_target]})

    assert {:ok, %{items: [summary]}} =
             RunStore.page_run_summaries(%PageRuns{
               scope: fixture.workspace_context,
               limit: 1
             })

    assert summary.run_id == run.id
    assert summary.target_label == "MyApp.Pipeline:daily"
    assert summary.target_refs == []
  end

  test "compact target lookup is scoped in SQL for duplicate cross-workspace run ids", fixture do
    other = provision_deploy_fixture(fixture.version)
    run_id = "shared-run-#{System.unique_integer([:positive])}"
    {command, _run} = create_run_command(fixture, run_id)
    {other_command, _other_run} = create_run_command(other, run_id)

    assert {:ok, _created} = RunStore.create_run(command)
    assert {:ok, _created} = RunStore.create_run(other_command)

    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn_storage_postgres, :repo, :query],
        fn _event, _measurements, metadata, pid ->
          if metadata.query =~ ~r/\bFROM\s+"favn_control"\."run_targets"/i do
            send(pid, {:run_target_row_count, query_row_count(metadata.result)})
          end
        end,
        self()
      )

    try do
      assert {:ok, %{items: [%{run_id: ^run_id}]}} =
               RunStore.page_run_summaries(%PageRuns{
                 scope: fixture.workspace_context,
                 limit: 1
               })

      assert_receive {:run_target_row_count, 1}
    after
      :telemetry.detach(handler_id)
    end
  end

  test "projection backfill restores missing rows without overwriting existing state",
       fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    assert {:ok, _publications} = Sequencer.sequence_batch()

    SQL.query!(
      Repo,
      "DELETE FROM favn_control.execution_group_overviews WHERE workspace_id = $1 AND root_run_id = $2",
      [fixture.workspace_id, run.id]
    )

    command = %BackfillMissingProjection{
      platform_context: fixture.platform_context,
      job_id: "projection-backfill:#{fixture.workspace_id}",
      projection: :execution_groups,
      workspace_id: fixture.workspace_id,
      limit: 250
    }

    assert {:ok,
            %{
              status: :completed,
              batch_count: 1,
              cursor: %{"publication_id" => publication_id}
            }} = MaintenanceStore.backfill_missing_projection(command)

    assert {:ok, %{status: :completed, batch_count: 0, processed_count: 1}} =
             MaintenanceStore.backfill_missing_projection(command)

    assert %{rows: [["pending", ^publication_id]]} =
             SQL.query!(
               Repo,
               "SELECT status, source_publication_id FROM favn_control.execution_group_overviews WHERE workspace_id = $1 AND root_run_id = $2",
               [fixture.workspace_id, run.id]
             )

    newer_publication_id = publication_id + 100

    SQL.query!(
      Repo,
      "UPDATE favn_control.execution_group_overviews SET status = 'failed', source_publication_id = $3 WHERE workspace_id = $1 AND root_run_id = $2",
      [fixture.workspace_id, run.id, newer_publication_id]
    )

    assert {:ok, %{batch_count: 1}} =
             MaintenanceStore.backfill_missing_projection(%{
               command
               | job_id: "projection-backfill-newer:#{fixture.workspace_id}"
             })

    assert %{rows: [["failed", ^newer_publication_id]]} =
             SQL.query!(
               Repo,
               "SELECT status, source_publication_id FROM favn_control.execution_group_overviews WHERE workspace_id = $1 AND root_run_id = $2",
               [fixture.workspace_id, run.id]
             )
  end

  test "cancellation request and API idempotency commit atomically and replay after terminal state",
       fixture do
    {create, run} =
      create_run_command(fixture, "cancel-idempotency-#{System.unique_integer([:positive])}")

    assert {:ok, _created} = RunStore.create_run(create)

    {:ok, idempotency} =
      CommandIdempotency.new(
        "run.cancel",
        :actor,
        "operator-a",
        :crypto.hash(:sha256, "cancel-key"),
        :crypto.hash(:sha256, "cancel-request"),
        DateTime.add(DateTime.utc_now(), 3_600, :second)
      )

    request = %RequestRunCancellation{
      workspace_context: fixture.workspace_context,
      command_id: "cancel:" <> run.id,
      run_id: run.id,
      reason: %{actor_id: "operator-a"},
      occurred_at: DateTime.utc_now(),
      idempotency: idempotency
    }

    assert {:ok, requested} = RunStore.request_cancellation(request)
    refute requested.replayed?
    assert requested.run.event_seq == 2
    assert requested.run.metadata.cancel_requested

    {cancelled, cancelled_event} =
      RunCancellation.finish(requested.run, request.reason, DateTime.utc_now())

    assert {:ok, terminal} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "cancel-finished:" <> run.id,
               expected_sequence: requested.run.event_seq,
               run: cancelled,
               event: FavnOrchestrator.RunEvent.to_map(cancelled_event)
             })

    assert terminal.run.status == :cancelled

    assert {:ok, replayed} =
             RunStore.request_cancellation(%{request | occurred_at: DateTime.utc_now()})

    assert replayed.replayed?
    assert replayed.run.status == :cancelled
    assert replayed.event.sequence == 2

    %{rows: [[1, 1]]} =
      SQL.query!(
        Repo,
        """
        SELECT
          count(*) FILTER (WHERE event_type = 'run_cancel_requested'),
          count(*) FILTER (WHERE event_kind = 'run.run_cancel_requested')
        FROM favn_control.run_events
        FULL JOIN favn_control.outbox_events USING (outbox_event_id)
        WHERE run_id = $1 OR aggregate_id = $1
        """,
        [run.id]
      )
  end

  test "orchestrator run use case writes only through the V2 capability registry", fixture do
    {_command, run} =
      create_run_command(fixture, "facade-run-#{System.unique_integer([:positive])}")

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               run,
               :run_submitted,
               %{status: :pending}
             )

    running = RunState.transition(run, status: :running)

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               running,
               :run_started,
               %{status: :running}
             )

    assert {:ok, ^running} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })
  end

  test "orchestrator run transitions carry the attached ownership fence", fixture do
    {_command, run} =
      create_run_command(fixture, "fenced-facade-run-#{System.unique_integer([:positive])}")

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               run,
               :run_submitted,
               %{status: :pending}
             )

    assert {:ok, first} =
             RunOwnership.claim(fixture.workspace_context, run.id, "worker-a",
               command_id: "claim-a:#{run.id}"
             )

    stale =
      run
      |> RunState.with_storage_fence(first.owner_id, first.fencing_token)
      |> RunState.transition(status: :running)

    assert :ok = RunOwnership.release(fixture.workspace_context, first)

    assert {:ok, second} =
             RunOwnership.claim(fixture.workspace_context, run.id, "worker-b",
               command_id: "claim-b:#{run.id}"
             )

    assert second.fencing_token > first.fencing_token

    assert {:error, %{kind: :fenced}} =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               stale,
               :run_started,
               %{status: :running}
             )

    current = RunState.with_storage_fence(stale, second.owner_id, second.fencing_token)

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               current,
               :run_started,
               %{status: :running}
             )
  end

  test "one release supports isolated workspace runs, configuration, and exact target catalogs",
       fixture do
    private_target_id = TargetStatus.target_id_for_asset({MyApp.PrivateAsset, :private})

    other =
      provision_deploy_fixture(fixture.version, [
        %DeploymentTarget{
          target_kind: :asset,
          target_id: private_target_id,
          selection_source: :explicit,
          customer_visible: true,
          descriptor: %{"target_id" => private_target_id, "label" => private_target_id}
        }
      ])

    run_id = "shared-run-identity"
    {first_command, first_run} = create_run_command(fixture, run_id)
    {second_command, second_run} = create_run_command(other, run_id)

    assert {:ok, _created} = RunStore.create_run(first_command)
    assert {:ok, _created} = RunStore.create_run(second_command)

    assert {:ok, ^first_run} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run_id
             })

    assert {:ok, ^second_run} =
             RunStore.get_run(%GetRun{
               workspace_context: other.workspace_context,
               run_id: run_id
             })

    assert fixture.deploy_command.configuration != other.deploy_command.configuration

    %{rows: catalogs} =
      SQL.query!(
        Repo,
        """
        SELECT workspace_id, array_agg(target_id ORDER BY target_id)
        FROM favn_control.workspace_deployment_targets
        WHERE workspace_id = ANY($1::text[]) AND target_kind = 'asset'
        GROUP BY workspace_id ORDER BY workspace_id
        """,
        [[fixture.workspace_id, other.workspace_id]]
      )

    catalog_by_workspace = Map.new(catalogs, fn [workspace_id, ids] -> {workspace_id, ids} end)
    refute private_target_id in catalog_by_workspace[fixture.workspace_id]
    assert private_target_id in catalog_by_workspace[other.workspace_id]
  end

  test "atomically persists encrypted manifest-bound runtime input pins", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    node_key = {{MyApp.Asset, :asset}, nil}

    {:ok, resolution} =
      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{
          account_id: 42,
          token: "must-remain-encrypted",
          date: ~D[2026-07-01],
          time: ~T[12:34:56.123456],
          naive_datetime: ~N[2026-07-01 12:34:56.123456],
          datetime: ~U[2026-07-01 12:34:56.123456Z],
          decimal: Decimal.new(-1, 12_340, -3)
        },
        input_identity: "input-42",
        metadata: %{source: :integration_test},
        sensitive_params: [:token]
      )

    pin = Pin.new(run.id, node_key, resolution)

    pin_command = %PinRuntimeInputs{
      workspace_context: fixture.workspace_context,
      command_id: "pin:" <> run.id,
      run_id: run.id,
      pins: [pin]
    }

    assert {:ok, [persisted]} = RunStore.pin_runtime_inputs(pin_command)
    assert persisted == pin
    assert {:ok, [^persisted]} = RunStore.pin_runtime_inputs(pin_command)

    previous_keys = Application.get_env(:favn_storage_postgres, :runtime_input_pin_keys)

    previous_version =
      Application.get_env(:favn_storage_postgres, :runtime_input_pin_current_key_version)

    on_exit(fn ->
      restore_env(:runtime_input_pin_keys, previous_keys)
      restore_env(:runtime_input_pin_current_key_version, previous_version)
    end)

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_keys, %{
      1 => "0123456789abcdef0123456789abcdef",
      2 => "abcdef0123456789abcdef0123456789"
    })

    Application.put_env(:favn_storage_postgres, :runtime_input_pin_current_key_version, 2)

    assert {:ok, [^persisted]} = RunStore.pin_runtime_inputs(pin_command)

    assert {:ok, [fetched]} =
             RunStore.get_runtime_inputs(%GetRuntimeInputs{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               node_keys: [node_key]
             })

    assert fetched == pin

    row = Repo.one!(from(stored in RuntimeInputPinRow, where: stored.run_id == ^run.id))
    refute row.payload =~ "must-remain-encrypted"
    assert byte_size(row.payload_fingerprint) == 32
    assert row.encryption_key_version == 1

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT key_version
               FROM favn_control.runtime_input_key_versions
               WHERE key_version IN (1, 2)
               ORDER BY key_version
               """,
               []
             )

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at)
      VALUES (98, clock_timestamp())
      """,
      []
    )

    assert {:error, {:runtime_input_key_versions_still_referenced, [1]}} =
             RuntimeInputKeyInventory.compact(Repo, [1])

    assert {:ok, [98]} = RuntimeInputKeyInventory.compact(Repo, [98])

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT key_version
               FROM favn_control.runtime_input_key_versions
               WHERE key_version IN (1, 98)
               ORDER BY key_version
               """,
               []
             )

    hidden_pin = Pin.new(run.id, {{MyApp.PrivateAsset, :private}, nil}, resolution)

    assert {:error, %{kind: :invalid}} =
             RunStore.pin_runtime_inputs(%{
               pin_command
               | command_id: "pin-hidden:" <> run.id,
                 pins: [hidden_pin]
             })

    conflicting =
      pin
      |> Map.put(:params, %{account_id: 43, token: "different"})
      |> Map.put(:payload_fingerprint, "different")

    assert {:error, %{kind: :conflict}} =
             RunStore.pin_runtime_inputs(%{pin_command | pins: [conflicting]})
  end

  test "runtime input pin creation is atomic with the current task assignment fence", fixture do
    {create_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(create_command)

    ref = {MyApp.Asset, :asset}
    node_key = {ref, nil}
    package = runtime_input_execution_package(ref)

    work = %Favn.Contracts.RunnerWork{
      run_id: run.id,
      manifest_version_id: fixture.version.manifest_version_id,
      manifest_content_hash: fixture.version.content_hash,
      required_runner_release_id: fixture.version.runner_releases["default"],
      runner_pool: :default,
      asset_ref: ref,
      asset_step_id: "runtime-input-step:" <> run.id,
      attempt: 1,
      execution_package: package,
      metadata: %{}
    }

    assert {:ok, queued, _work} =
             FavnOrchestrator.AssetRunnerTasks.enqueue(
               run,
               work,
               node_key,
               0,
               1,
               %{continuation: :runtime_input_fence_test}
             )

    old_claim = %ClaimRunnerTask{
      platform_context:
        SystemContext.platform(:runtime_input_fence_test, roles: [:platform_operator]),
      command_id: "runtime-input-old-claim:" <> run.id,
      runner_instance_id: "runtime-input-old-runner:" <> run.id,
      runner_session_generation: 11,
      runner_pool: "default",
      required_runner_release_id: fixture.version.runner_releases["default"],
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"],
      lease_duration_ms: 30_000,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, old_assignment} =
             FavnStoragePostgres.RunnerTasks.Store.claim(old_claim)

    assert {:ok, _requeued} =
             FavnStoragePostgres.RunnerTasks.Store.release(
               %FavnOrchestrator.Persistence.Commands.ReleaseRunnerTask{
                 workspace_context: fixture.workspace_context,
                 command_id: "runtime-input-release:" <> run.id,
                 task_id: queued.task_id,
                 runner_instance_id: old_assignment.assigned_runner_instance_id,
                 runner_session_generation: old_assignment.assigned_runner_session_generation,
                 assignment_generation: old_assignment.assignment_generation,
                 disposition: :requeue,
                 reason: :test_reassignment,
                 issued_at: DateTime.utc_now(),
                 occurred_at: DateTime.utc_now()
               }
             )

    current_claim = %{
      old_claim
      | command_id: "runtime-input-current-claim:" <> run.id,
        runner_instance_id: "runtime-input-current-runner:" <> run.id,
        runner_session_generation: 22,
        issued_at: DateTime.utc_now(),
        occurred_at: DateTime.utc_now()
    }

    assert {:ok, current_assignment} =
             FavnStoragePostgres.RunnerTasks.Store.claim(current_claim)

    {:ok, stale_resolution} =
      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: 1, token: "stale-secret"},
        input_identity: "stale-input",
        sensitive_params: [:token]
      )

    {:ok, current_resolution} =
      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: 2, token: "current-secret"},
        input_identity: "current-input",
        sensitive_params: [:token]
      )

    stale_pin = Pin.new(run.id, node_key, stale_resolution)
    current_pin = Pin.new(run.id, node_key, current_resolution)

    command = fn assignment, suffix, pin ->
      %FavnOrchestrator.Persistence.Commands.PersistRunnerTaskRuntimeInputs{
        workspace_context: fixture.workspace_context,
        command_id: "runtime-input-persist:#{suffix}:" <> run.id,
        task_id: queued.task_id,
        runner_instance_id: assignment.assigned_runner_instance_id,
        runner_session_generation: assignment.assigned_runner_session_generation,
        assignment_generation: assignment.assignment_generation,
        resolution_id: "resolution:#{suffix}:" <> run.id,
        status: :resolved,
        payload_fingerprint: Base.decode16!(pin.payload_fingerprint, case: :mixed),
        runtime_input_pin: pin,
        error: nil,
        issued_at: DateTime.utc_now(),
        occurred_at: DateTime.utc_now()
      }
    end

    parent = self()

    stale =
      Task.async(fn ->
        receive do
          :go -> :ok
        end

        send(parent, :stale_started)

        FavnStoragePostgres.RunnerTasks.Store.persist_runtime_inputs(
          command.(old_assignment, "stale", stale_pin)
        )
      end)

    current =
      Task.async(fn ->
        receive do
          :go -> :ok
        end

        send(parent, :current_started)

        FavnStoragePostgres.RunnerTasks.Store.persist_runtime_inputs(
          command.(current_assignment, "current", current_pin)
        )
      end)

    send(stale.pid, :go)
    send(current.pid, :go)
    assert_receive :stale_started
    assert_receive :current_started

    assert {:error, %{kind: :fenced}} = Task.await(stale)
    assert {:ok, persisted_task} = Task.await(current)

    assert persisted_task.runtime_input_payload_fingerprint ==
             Base.decode16!(current_pin.payload_fingerprint, case: :mixed)

    assert {:ok, [^current_pin]} =
             RunStore.get_runtime_inputs(%GetRuntimeInputs{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               node_keys: [node_key]
             })

    # A resolved resolution is pinned and stays protected: the same fenced
    # assignment cannot replace it with different inputs.
    {:ok, replacement_resolution} =
      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: 3, token: "replacement-secret"},
        input_identity: "replacement-input",
        sensitive_params: [:token]
      )

    replacement_pin = Pin.new(run.id, node_key, replacement_resolution)

    assert {:error, %{kind: :conflict}} =
             FavnStoragePostgres.RunnerTasks.Store.persist_runtime_inputs(
               command.(current_assignment, "replacement", replacement_pin)
             )
  end

  test "runtime input package lookup matches exact asset pairs", fixture do
    requested_refs = [{MyApp.Asset, :asset}, {MyApp.PrivateAsset, :private}]

    all_refs =
      requested_refs ++ [{MyApp.Asset, :private}, {MyApp.PrivateAsset, :asset}]

    packages = Enum.map(all_refs, &runtime_input_execution_package/1)

    assets =
      Enum.map(packages, fn package ->
        {module, name} = package.asset_ref

        %Favn.Manifest.Asset{
          ref: package.asset_ref,
          module: module,
          name: name,
          type: :sql,
          execution_package_hash: package.content_hash
        }
      end)

    {:ok, version} =
      Version.new(
        FavnTestSupport.with_manifest_contract(%Manifest{
          assets: assets,
          graph: %Graph{nodes: all_refs, topo_order: all_refs}
        }),
        manifest_version_id: "mv-crossed-#{System.unique_integer([:positive])}"
      )

    assert :ok =
             RegistryStore.register_execution_packages(%RegisterExecutionPackages{
               platform_context: fixture.platform_context,
               packages: packages
             })

    assert {:ok, ^version} =
             RegistryStore.register_manifest(%RegisterManifest{
               platform_context: fixture.platform_context,
               version: version
             })

    deployment_id = "deploy-crossed-#{System.unique_integer([:positive])}"

    targets =
      Enum.map(requested_refs, fn ref ->
        %DeploymentTarget{
          target_kind: :asset,
          target_id: TargetStatus.target_id_for_asset(ref),
          selection_source: :explicit,
          customer_visible: false,
          descriptor: %{
            "target_id" => TargetStatus.target_id_for_asset(ref),
            "label" => inspect(ref)
          }
        }
      end)

    assert {:ok, _runtime} =
             RegistryStore.deploy_manifest(%DeployManifest{
               platform_context: fixture.platform_context,
               workspace_context: fixture.workspace_context,
               deployment_id: deployment_id,
               manifest_version_id: version.manifest_version_id,
               configuration: fixture.deploy_command.configuration,
               targets: targets,
               schedules: [],
               capacity_scopes: [],
               occurred_at: DateTime.utc_now()
             })

    scoped_fixture = %{
      fixture
      | deployment_id: deployment_id,
        version: version,
        target_id: TargetStatus.target_id_for_asset({MyApp.Asset, :asset})
    }

    {command, run} = create_run_command(scoped_fixture)

    private_target = %RunTarget{
      target_kind: :asset,
      target_id: TargetStatus.target_id_for_asset({MyApp.PrivateAsset, :private}),
      target_module: "MyApp.PrivateAsset",
      target_name: "private",
      is_primary: false
    }

    run = %{run | target_refs: requested_refs} |> RunState.with_snapshot_hash()

    assert {:ok, _created} =
             RunStore.create_run(%{
               command
               | run: run,
                 targets: command.targets ++ [private_target]
             })

    {:ok, resolution} =
      Resolution.new(
        resolver: MyApp.RuntimeInputResolver,
        params: %{account_id: 42},
        input_identity: "crossed-input",
        metadata: %{},
        sensitive_params: []
      )

    pins = Enum.map(requested_refs, &Pin.new(run.id, {&1, nil}, resolution))

    assert {:ok, persisted} =
             RunStore.pin_runtime_inputs(%PinRuntimeInputs{
               workspace_context: fixture.workspace_context,
               command_id: "pin-crossed:" <> run.id,
               run_id: run.id,
               pins: pins
             })

    assert Enum.sort_by(persisted, & &1.node_key) == Enum.sort_by(pins, & &1.node_key)
  end

  test "fences run ownership", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    claim = %ClaimRun{
      workspace_context: fixture.workspace_context,
      command_id: "claim:" <> run.id,
      run_id: run.id,
      owner_id: "node-a",
      lease_duration_ms: 30_000
    }

    assert {:ok, ownership} = RunOwnershipStore.claim_run(claim)
    assert ownership.fencing_token == 1
    assert {:ok, ^ownership} = RunOwnershipStore.claim_run(claim)

    renewal = %RenewRunOwnership{
      workspace_context: fixture.workspace_context,
      renewal_id: "renew:" <> run.id <> ":1",
      run_id: run.id,
      owner_id: "node-a",
      fencing_token: ownership.fencing_token,
      lease_duration_ms: 30_000
    }

    assert {:ok, renewed} = RunOwnershipStore.renew_run(renewal)
    assert {:ok, ^renewed} = RunOwnershipStore.renew_run(renewal)

    assert :ok =
             RunOwnershipStore.release_run(%ReleaseRunOwnership{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               owner_id: "node-a",
               fencing_token: ownership.fencing_token
             })

    assert {:error, %{kind: :fenced}} =
             RunOwnershipStore.renew_run(%{
               renewal
               | renewal_id: "renew-after-release:" <> run.id
             })
  end

  test "stores one fenced, exact-replay-safe execution checkpoint per run", fixture do
    {create, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(create)

    assert {:ok, ownership} =
             RunOwnershipStore.claim_run(%ClaimRun{
               workspace_context: fixture.workspace_context,
               command_id: "claim-checkpoint:" <> run.id,
               run_id: run.id,
               owner_id: "checkpoint-owner",
               lease_duration_ms: 30_000
             })

    payload = :erlang.term_to_binary(%{format: :checkpoint, states: []}, [:deterministic])

    command = %PutRunExecutionCheckpoint{
      workspace_context: fixture.workspace_context,
      run_id: run.id,
      owner_id: ownership.owner_id,
      fencing_token: ownership.fencing_token,
      checkpoint_version: 1,
      checkpoint_revision: 1,
      checkpoint_sequence: run.event_seq,
      stage: 0,
      attempt: 1,
      payload: payload,
      payload_hash: :crypto.hash(:sha256, payload),
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, checkpoint} = RunStore.put_execution_checkpoint(command)
    assert checkpoint.payload == payload
    assert checkpoint.checkpoint_revision == 1
    assert checkpoint.checkpoint_sequence == run.event_seq
    assert checkpoint.stage == 0
    assert checkpoint.attempt == 1
    assert {:ok, ^checkpoint} = RunStore.put_execution_checkpoint(command)

    assert {:ok, ^checkpoint} =
             RunStore.get_execution_checkpoint(%GetRunExecutionCheckpoint{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })

    different_payload = :erlang.term_to_binary(%{format: :checkpoint, states: [:changed]})

    assert {:error, %{kind: :conflict}} =
             RunStore.put_execution_checkpoint(%{
               command
               | payload: different_payload,
                 payload_hash: :crypto.hash(:sha256, different_payload)
             })

    running = RunState.transition(run, status: :running)

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "transition-checkpoint:" <> run.id,
               expected_sequence: run.event_seq,
               owner_id: ownership.owner_id,
               fencing_token: ownership.fencing_token,
               run: running,
               event: %{
                 run_id: run.id,
                 sequence: running.event_seq,
                 event_type: :run_started,
                 status: :running,
                 occurred_at: DateTime.utc_now()
               }
             })

    advanced = %{
      command
      | checkpoint_revision: 2,
        checkpoint_sequence: running.event_seq,
        stage: 1,
        payload: different_payload,
        payload_hash: :crypto.hash(:sha256, different_payload),
        occurred_at: DateTime.utc_now()
    }

    assert {:ok, advanced_checkpoint} = RunStore.put_execution_checkpoint(advanced)
    assert advanced_checkpoint.checkpoint_revision == 2
    assert advanced_checkpoint.checkpoint_sequence == running.event_seq
    assert advanced_checkpoint.stage == 1
    assert advanced_checkpoint.payload == different_payload
    assert {:error, %{kind: :conflict}} = RunStore.put_execution_checkpoint(command)

    same_sequence_next_stage = %{
      advanced
      | checkpoint_revision: 3,
        stage: 2,
        occurred_at: DateTime.utc_now()
    }

    assert {:ok, next_stage_checkpoint} =
             RunStore.put_execution_checkpoint(same_sequence_next_stage)

    assert next_stage_checkpoint.checkpoint_revision == 3
    assert next_stage_checkpoint.checkpoint_sequence == running.event_seq
    assert next_stage_checkpoint.stage == 2

    assert {:error, %{kind: :fenced}} =
             RunStore.put_execution_checkpoint(%{
               same_sequence_next_stage
               | fencing_token: ownership.fencing_token + 1
             })
  end

  test "run detail fetches an active retry checkpoint beyond the first event page", fixture do
    {command, run} = pipeline_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    node_key = {{MyApp.Asset, :asset}, nil}

    noisy_run =
      Enum.reduce(1..205, run, fn index, current ->
        next =
          RunState.transition(current,
            status: :running,
            metadata: Map.put(current.metadata, :projection_noise, index)
          )

        assert :ok =
                 TransitionWriter.persist_transition(
                   fixture.workspace_context,
                   next,
                   :step_queued,
                   %{
                     node_key: node_key,
                     asset_ref: {MyApp.Asset, :asset},
                     stage: 0,
                     attempt: 1,
                     queue_reason: :test
                   }
                 )

        next
      end)

    checkpoint_sequence = noisy_run.event_seq + 1
    {:ok, selection} = PipelineRetryCheckpoint.encode([node_key], [node_key])

    retrying =
      RunState.transition(noisy_run,
        status: :running,
        metadata:
          Map.merge(noisy_run.metadata, %{
            retrying: true,
            next_attempt: 2,
            retry_state: %{
              kind: :pipeline,
              checkpoint_sequence: checkpoint_sequence,
              stage_index: 0,
              next_attempt: 2,
              stage: 0,
              next_retry_at: System.system_time(:millisecond) + 30_000
            }
          })
      )

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               retrying,
               :pipeline_retry_checkpointed,
               %{
                 stage: 0,
                 attempt: 1,
                 next_attempt: 2,
                 retry_selection: selection
               }
             )

    assert checkpoint_sequence > 200
    assert {:ok, detail} = RunReadModel.get_run_detail(fixture.workspace_context, run.id)
    assert [%{node_key: ^node_key, status: :retrying, attempt: 2}] = detail.steps
  end

  test "orchestrator admission uses the run-scoped PostgreSQL capacity counter", fixture do
    {command, run} = create_run_command(fixture)

    run =
      %{run | metadata: %{pipeline_execution_policy: %{max_concurrency: 1}}}
      |> RunState.with_snapshot_hash()

    assert {:ok, _created} = RunStore.create_run(%{command | run: run})

    owner_id = "admission:" <> run.id

    assert {:ok, authority} =
             RunOwnership.claim(fixture.workspace_context, run.id, owner_id,
               command_id: "claim-admission:" <> run.id
             )

    run = RunState.with_storage_fence(run, authority.owner_id, authority.fencing_token)

    assert {:ok, lease} =
             ExecutionAdmission.acquire(run, %{
               asset_step_id: "step-one:" <> run.id,
               execution_pool: nil
             })

    assert lease.workspace_id == fixture.workspace_id
    assert [%{kind: :run, limit: 1}] = lease.scopes

    assert {:queued, :pipeline_concurrency, %{kind: :run}} =
             ExecutionAdmission.acquire(run, %{
               asset_step_id: "step-two:" <> run.id,
               execution_pool: nil
             })

    assert :ok = ExecutionAdmission.release(lease)

    assert {:ok, second_lease} =
             ExecutionAdmission.acquire(run, %{
               asset_step_id: "step-two:" <> run.id,
               execution_pool: nil
             })

    assert :ok = ExecutionAdmission.release(second_lease)
  end

  test "pipeline continues independent branches after a terminal sibling failure", fixture do
    asset = Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))
    {plan, keys} = continuation_regression_plan(asset.semantic_generation_id)
    {command, original} = pipeline_run_command(fixture)

    run =
      RunState.new(
        id: original.id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        runner_releases: fixture.version.runner_releases,
        asset_ref: original.asset_ref,
        target_refs: original.target_refs,
        submit_kind: :pipeline,
        plan: plan,
        metadata: %{pipeline_execution_policy: %{max_concurrency: 1}}
      )

    command = %{command | run: run, event: %{command.event | occurred_at: run.inserted_at}}
    assert {:ok, _created} = RunStore.create_run(command)

    {:ok, runner_state} =
      Agent.start_link(fn -> %{work: %{}, submitted: [], fail_key: keys.b} end)

    share_repo_sandbox!()
    start_supervised!({FavnOrchestrator.ExecutionAdmission.Coordinator, []})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})

    runner = spawn_link(fn -> durable_pipeline_runner(runner_state, fixture, keys.b) end)
    assert {:ok, pid} = RunServer.start_link(%{run_state: run, version: fixture.version})
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    send(runner, :stop)

    assert {:ok, finished} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })

    assert finished.status == :error
    node_results = finished.result.node_results

    assert length(node_results) == 5

    statuses = Map.new(node_results, &{&1.node_key, &1.status})
    assert statuses[keys.a] == :ok
    assert statuses[keys.b] == :error
    assert statuses[keys.c] == :ok
    assert statuses[keys.d] == :blocked
    assert statuses[keys.e] == :ok

    submitted = Agent.get(runner_state, &Enum.reverse(&1.submitted))
    assert Enum.count(submitted, &(&1 == keys.b)) == 2
    assert keys.c in submitted
    assert keys.e in submitted
    refute keys.d in submitted
  end

  test "all-runnable second stage advances its checkpoint at the same run sequence", fixture do
    asset = Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))
    {plan, keys} = continuation_regression_plan(asset.semantic_generation_id)
    {command, original} = pipeline_run_command(fixture)

    run =
      RunState.new(
        id: original.id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        runner_releases: fixture.version.runner_releases,
        asset_ref: original.asset_ref,
        target_refs: original.target_refs,
        submit_kind: :pipeline,
        plan: plan,
        metadata: %{pipeline_execution_policy: %{max_concurrency: 1}}
      )

    command = %{command | run: run, event: %{command.event | occurred_at: run.inserted_at}}
    assert {:ok, _created} = RunStore.create_run(command)

    {:ok, runner_state} =
      Agent.start_link(fn -> %{work: %{}, submitted: [], fail_key: :never} end)

    share_repo_sandbox!()
    start_supervised!({FavnOrchestrator.ExecutionAdmission.Coordinator, []})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})

    runner = spawn_link(fn -> durable_pipeline_runner(runner_state, fixture, :never) end)
    assert {:ok, pid} = RunServer.start_link(%{run_state: run, version: fixture.version})
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000
    send(runner, :stop)

    assert {:ok, finished} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })

    assert finished.status == :ok
    assert Enum.all?(finished.result.node_results, &(&1.status == :ok))

    submitted = Agent.get(runner_state, &Enum.reverse(&1.submitted))
    assert Enum.sort(submitted) == Enum.sort(Map.values(keys))
  end

  defp durable_pipeline_runner(state, fixture, fail_key) do
    receive do
      :stop ->
        :ok
    after
      0 ->
        case claim_asset_task(fixture) do
          {:ok, nil} ->
            Process.sleep(5)

          {:ok, task} ->
            work = task.payload
            node_key = Favn.Contracts.RunnerWork.node_key(work)
            Agent.update(state, &%{&1 | submitted: [node_key | &1.submitted]})
            :ok = complete_asset_task(task, work, node_key == fail_key)

          {:error, _reason} ->
            Process.sleep(5)
        end

        durable_pipeline_runner(state, fixture, fail_key)
    end
  end

  defp claim_asset_task(fixture) do
    FavnStoragePostgres.RunnerTasks.Store.claim(%ClaimRunnerTask{
      platform_context:
        SystemContext.platform(:pipeline_runner_test, roles: [:platform_operator]),
      command_id: "pipeline-claim:#{System.unique_integer([:positive, :monotonic])}",
      runner_instance_id: "pipeline-runner:#{fixture.workspace_id}",
      runner_session_generation: 1,
      runner_pool: "default",
      required_runner_release_id: fixture.version.runner_releases["default"],
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"],
      lease_duration_ms: 30_000,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    })
  end

  defp complete_asset_task(task, work, fail?) do
    {outcome, retry_class, status, error} =
      if fail? do
        retryable? = work.attempt == 1

        error =
          Favn.Contracts.RunnerError.new(
            type: :fixture_failure,
            message: "fixture failure",
            retryable?: retryable?,
            outcome: if(retryable?, do: :safe_failure, else: :unknown)
          )

        {:failed, if(retryable?, do: :safe_to_retry, else: :terminal), :error, error}
      else
        {:succeeded, :terminal, :ok, nil}
      end

    result = %Favn.Contracts.RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: status,
      asset_results: [],
      error: error,
      metadata: %{}
    }

    case RunnerTasks.complete(%Favn.Contracts.RunnerTask.Result{
           workspace_id: task.workspace_id,
           task_id: task.task_id,
           task_kind: task.task_kind,
           runner_instance_id: task.assigned_runner_instance_id,
           runner_session_generation: task.assigned_runner_session_generation,
           assignment_generation: task.assignment_generation,
           outcome: outcome,
           retry_class: retry_class,
           result: if(outcome == :succeeded, do: result, else: nil),
           error: error,
           finished_at: DateTime.utc_now()
         }) do
      {:ok, _ack} -> :ok
      {:error, reason} -> raise "failed to complete durable test task: #{inspect(reason)}"
    end
  end

  test "schedule activation replays return the immutable original receipt", fixture do
    assert {:ok, %{items: [schedule]}} =
             FavnOrchestrator.Operator.Schedules.page_entries(
               fixture.workspace_context,
               limit: 10
             )

    first_time = ~U[2026-07-25 09:00:00.123456Z]

    assert {:ok, activated} =
             FavnOrchestrator.Operator.Schedules.activate(
               fixture.workspace_context,
               schedule.id,
               "operator-a",
               "reviewed",
               command_id: "receipt-activate:" <> fixture.workspace_id,
               now: first_time
             )

    assert activated.previous_state == :disabled
    assert activated.effective_state == :enabled
    assert activated.command_time == first_time

    assert {:ok, _deactivated} =
             FavnOrchestrator.Operator.Schedules.deactivate(
               fixture.workspace_context,
               schedule.id,
               "operator-a",
               "maintenance",
               command_id: "receipt-deactivate:" <> fixture.workspace_id,
               now: DateTime.add(first_time, 60, :second)
             )

    assert {:ok, ^activated} =
             FavnOrchestrator.Operator.Schedules.activate(
               fixture.workspace_context,
               schedule.id,
               "operator-a",
               "reviewed",
               command_id: "receipt-activate:" <> fixture.workspace_id,
               now: DateTime.add(first_time, 120, :second)
             )

    assert {:ok, current} =
             FavnOrchestrator.Operator.Schedules.get_entry(
               fixture.workspace_context,
               schedule.id
             )

    assert current.activation_state == :disabled
  end

  test "schedule activation HTTP retries return the original receipt", fixture do
    identity = api_identity(fixture, [:operator])

    assert {:ok, %{items: [schedule]}} =
             FavnOrchestrator.Operator.Schedules.page_entries(
               fixture.workspace_context,
               limit: 10
             )

    path = "/api/orchestrator/v1/schedules/#{schedule.id}"

    activated =
      api_request(:post, path <> "/activate", %{"reason" => "reviewed"},
        fixture: fixture,
        identity: identity,
        idempotency_key: "http-activate"
      )

    assert activated.status == 200
    activated_data = JSON.decode!(activated.resp_body)["data"]
    assert activated_data["previous_state"] == "disabled"
    assert activated_data["effective_state"] == "enabled"

    deactivated =
      api_request(:post, path <> "/deactivate", %{"reason" => "maintenance"},
        fixture: fixture,
        identity: identity,
        idempotency_key: "http-deactivate"
      )

    assert deactivated.status == 200

    replayed =
      api_request(:post, path <> "/activate", %{"reason" => "reviewed"},
        fixture: fixture,
        identity: identity,
        idempotency_key: "http-activate"
      )

    assert replayed.status == 200
    assert JSON.decode!(replayed.resp_body)["data"] == activated_data

    current =
      api_request(:get, path, nil,
        fixture: fixture,
        identity: identity
      )

    assert current.status == 200
    assert JSON.decode!(current.resp_body)["data"]["schedule"]["activation_state"] == "disabled"
  end

  test "claims schedules and dispatches deterministic occurrence intents", fixture do
    assert {:ok, schedule_page} =
             SchedulerStore.page_schedules(%PageSchedules{
               workspace_context: fixture.workspace_context,
               limit: 10
             })

    assert [schedule] = schedule_page.items
    assert schedule.pipeline_target_id == fixture.pipeline_target_id
    assert schedule.schedule_fingerprint == "schedule-fingerprint-daily"
    assert schedule.definition["cron"] == "0 0 * * *"

    assert {:ok, operator_page} =
             FavnOrchestrator.Operator.Schedules.page_entries(
               fixture.workspace_context,
               limit: 10
             )

    assert [operator_schedule] = operator_page.items
    assert operator_schedule.pipeline_module == MyApp.Pipeline
    assert operator_schedule.activation_state == :disabled

    assert {:ok, []} =
             SchedulerStore.claim_due_schedules(%ClaimDueSchedules{
               workspace_context: fixture.workspace_context,
               batch_id: "disabled-schedule-claim:" <> fixture.workspace_id,
               owner_id: "scheduler-a",
               lease_duration_ms: 30_000,
               limit: 10
             })

    activated_at = DateTime.utc_now()

    activation = %SetScheduleActivation{
      workspace_context: fixture.workspace_context,
      pipeline_target_id: fixture.pipeline_target_id,
      schedule_id: "daily",
      schedule_fingerprint: schedule.schedule_fingerprint,
      enabled: true,
      actor_id: "operator-a",
      reason: "reviewed in storage test",
      command_id: "schedule-activate:" <> fixture.workspace_id,
      request_hash: :crypto.hash(:sha256, "activate:" <> fixture.workspace_id),
      occurred_at: activated_at,
      next_due_at: DateTime.add(activated_at, -1, :second)
    }

    assert {:ok, reader_context} =
             WorkspaceContext.new(
               fixture.workspace_id,
               "schedule-reader",
               [:customer_reader]
             )

    assert {:error, %{kind: :invalid}} =
             SchedulerStore.set_activation(%{activation | workspace_context: reader_context})

    assert {:ok, activated} = SchedulerStore.set_activation(activation)
    assert activated.enabled
    assert {:ok, ^activated} = SchedulerStore.set_activation(activation)

    assert {:error, %{kind: :conflict}} =
             SchedulerStore.set_activation(%{
               activation
               | enabled: false,
                 request_hash: :crypto.hash(:sha256, "deactivate:" <> fixture.workspace_id)
             })

    assert {:ok, enabled_page} =
             FavnOrchestrator.Operator.Schedules.page_entries(
               fixture.workspace_context,
               limit: 10
             )

    assert [%{activation_state: :enabled, effective_enabled?: true}] = enabled_page.items

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.schedule_cursors
      SET schedule_fingerprint = 'changed-schedule-fingerprint'
      WHERE workspace_id = $1 AND deployment_id = $2
        AND pipeline_target_id = $3 AND schedule_id = 'daily'
      """,
      [fixture.workspace_id, fixture.deployment_id, fixture.pipeline_target_id]
    )

    assert {:ok, changed_page} =
             FavnOrchestrator.Operator.Schedules.page_entries(
               fixture.workspace_context,
               limit: 10
             )

    assert [%{activation_state: :needs_review, effective_enabled?: false}] =
             changed_page.items

    assert {:ok, []} =
             SchedulerStore.claim_due_schedules(%ClaimDueSchedules{
               workspace_context: fixture.workspace_context,
               batch_id: "changed-schedule-claim:" <> fixture.workspace_id,
               owner_id: "scheduler-a",
               lease_duration_ms: 30_000,
               limit: 10
             })

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.schedule_cursors
      SET schedule_fingerprint = $4
      WHERE workspace_id = $1 AND deployment_id = $2
        AND pipeline_target_id = $3 AND schedule_id = 'daily'
      """,
      [
        fixture.workspace_id,
        fixture.deployment_id,
        fixture.pipeline_target_id,
        schedule.schedule_fingerprint
      ]
    )

    claim_command = %ClaimDueSchedules{
      workspace_context: fixture.workspace_context,
      batch_id: "schedule-claim:" <> fixture.workspace_id,
      owner_id: "scheduler-a",
      lease_duration_ms: 30_000,
      limit: 10
    }

    assert {:ok, [claim]} = SchedulerStore.claim_due_schedules(claim_command)
    assert claim.pipeline_target_id == fixture.pipeline_target_id
    assert {:ok, [^claim]} = SchedulerStore.claim_due_schedules(claim_command)

    occurrence_id = "occurrence:" <> fixture.workspace_id
    occurred_at = DateTime.utc_now()

    evaluation = %CommitScheduleEvaluation{
      workspace_context: fixture.workspace_context,
      command_id: "schedule-evaluation:" <> fixture.workspace_id,
      deployment_id: fixture.deployment_id,
      pipeline_target_id: fixture.pipeline_target_id,
      schedule_id: "daily",
      owner_id: "scheduler-a",
      claim_generation: claim.claim_generation,
      expected_version: claim.version,
      next_due_at: DateTime.add(occurred_at, 86_400, :second),
      cursor: %{"last_due_at" => DateTime.to_iso8601(occurred_at)},
      occurrences: [
        %ScheduleOccurrenceIntent{
          occurrence_id: occurrence_id,
          due_at: occurred_at,
          payload: %{"trigger" => "daily"}
        }
      ],
      occurred_at: occurred_at
    }

    assert {:ok, [occurrence]} = SchedulerStore.commit_evaluation(evaluation)
    assert occurrence.status == :pending
    assert {:ok, [^occurrence]} = SchedulerStore.commit_evaluation(evaluation)

    assert {:ok, occurrence_page} =
             SchedulerStore.page_occurrences(%PageScheduleOccurrences{
               workspace_context: fixture.workspace_context,
               pipeline_target_id: fixture.pipeline_target_id,
               schedule_id: "daily",
               limit: 10
             })

    assert [persisted_occurrence] = occurrence_page.items
    assert persisted_occurrence.occurrence_id == occurrence_id

    occurrence_claim = %ClaimScheduleOccurrences{
      workspace_context: fixture.workspace_context,
      batch_id: "occurrence-claim:" <> fixture.workspace_id,
      owner_id: "scheduler-a",
      lease_duration_ms: 30_000,
      limit: 10
    }

    assert {:ok, [claimed]} = SchedulerStore.claim_occurrences(occurrence_claim)
    assert claimed.status == :claimed
    assert claimed.claim_generation == 1
    assert {:ok, [^claimed]} = SchedulerStore.claim_occurrences(occurrence_claim)

    authorization = %AuthorizeScheduleOccurrenceDispatch{
      workspace_context: fixture.workspace_context,
      command_id: "occurrence-dispatch:" <> fixture.workspace_id,
      submission: schedule_submission(fixture, "primary"),
      occurrence_id: claimed.occurrence_id,
      pipeline_target_id: fixture.pipeline_target_id,
      schedule_id: "daily",
      schedule_fingerprint: schedule.schedule_fingerprint,
      owner_id: claimed.claim_owner,
      claim_generation: claimed.claim_generation,
      occurred_at: DateTime.utc_now()
    }

    assert {:error, %{kind: :fenced}} =
             SchedulerStore.authorize_occurrence_dispatch(%{
               authorization
               | command_id: "occurrence-dispatch-wrong-identity:" <> fixture.workspace_id,
                 schedule_id: "other-schedule"
             })

    assert {:ok, authorized} = SchedulerStore.authorize_occurrence_dispatch(authorization)
    assert authorized.status == :completed
    assert authorized.run_id == authorization.submission.run_id
    assert {:ok, ^authorized} = SchedulerStore.authorize_occurrence_dispatch(authorization)

    assert {:ok, queued_submission} =
             RunSubmissionStore.get(%FavnOrchestrator.Persistence.Queries.GetRunSubmission{
               workspace_context: fixture.workspace_context,
               submission_id: authorization.submission.submission_id
             })

    assert queued_submission.status == :queued
    assert queued_submission.run_id == authorization.submission.run_id

    deactivation = %SetScheduleActivation{
      activation
      | enabled: false,
        reason: "maintenance",
        command_id: "schedule-deactivate:" <> fixture.workspace_id,
        request_hash: :crypto.hash(:sha256, "maintenance:" <> fixture.workspace_id),
        occurred_at: DateTime.utc_now(),
        next_due_at: nil
    }

    assert {:ok, deactivated} = SchedulerStore.set_activation(deactivation)
    refute deactivated.enabled
    assert {:ok, ^activated} = SchedulerStore.set_activation(activation)

    assert {:ok, %{items: [%{activation_enabled: false, next_due_at: nil}]}} =
             SchedulerStore.page_schedules(%PageSchedules{
               workspace_context: fixture.workspace_context,
               limit: 10
             })

    assert {:ok, occurrence_after_deactivation} =
             SchedulerStore.page_occurrences(%PageScheduleOccurrences{
               workspace_context: fixture.workspace_context,
               pipeline_target_id: fixture.pipeline_target_id,
               schedule_id: "daily",
               limit: 10
             })

    assert [%{status: :completed, run_id: run_id}] = occurrence_after_deactivation.items
    assert run_id == authorization.submission.run_id

    %{rows: replay_rows} =
      SQL.query!(
        Repo,
        "SELECT status, claim_owner, claim_command_id FROM favn_control.schedule_occurrences WHERE workspace_id = $1 AND occurrence_id = $2",
        [fixture.workspace_id, occurrence_id]
      )

    assert replay_rows == [
             [
               "completed",
               claimed.claim_owner,
               "occurrence-claim:" <> fixture.workspace_id
             ]
           ]

    assert {:ok, []} =
             SchedulerStore.claim_occurrences(%{
               occurrence_claim
               | batch_id: "occurrence-empty-claim:" <> fixture.workspace_id
             })
  end

  test "deactivation suppresses claimed work before dispatch authorization", fixture do
    assert {:ok, %{items: [schedule]}} =
             SchedulerStore.page_schedules(%PageSchedules{
               workspace_context: fixture.workspace_context,
               limit: 10
             })

    now = DateTime.utc_now()

    activation = %SetScheduleActivation{
      workspace_context: fixture.workspace_context,
      pipeline_target_id: fixture.pipeline_target_id,
      schedule_id: "daily",
      schedule_fingerprint: schedule.schedule_fingerprint,
      enabled: true,
      actor_id: "operator-a",
      reason: "race test",
      command_id: "schedule-race-activate:" <> fixture.workspace_id,
      request_hash: :crypto.hash(:sha256, "race-activate:" <> fixture.workspace_id),
      occurred_at: now,
      next_due_at: DateTime.add(now, -1, :second)
    }

    assert {:ok, _activation} = SchedulerStore.set_activation(activation)

    assert {:ok, [claim]} =
             SchedulerStore.claim_due_schedules(%ClaimDueSchedules{
               workspace_context: fixture.workspace_context,
               batch_id: "schedule-race-claim:" <> fixture.workspace_id,
               owner_id: "scheduler-race",
               lease_duration_ms: 30_000,
               limit: 10
             })

    occurrence_id = "occurrence-race:" <> fixture.workspace_id

    assert {:ok, [_occurrence]} =
             SchedulerStore.commit_evaluation(%CommitScheduleEvaluation{
               workspace_context: fixture.workspace_context,
               command_id: "schedule-race-evaluation:" <> fixture.workspace_id,
               deployment_id: fixture.deployment_id,
               pipeline_target_id: fixture.pipeline_target_id,
               schedule_id: "daily",
               owner_id: claim.owner_id,
               claim_generation: claim.claim_generation,
               expected_version: claim.version,
               next_due_at: DateTime.add(now, 86_400, :second),
               cursor: %{},
               occurrences: [
                 %ScheduleOccurrenceIntent{
                   occurrence_id: occurrence_id,
                   due_at: now,
                   payload: %{"trigger" => "race"}
                 }
               ],
               occurred_at: now
             })

    assert {:ok, [claimed]} =
             SchedulerStore.claim_occurrences(%ClaimScheduleOccurrences{
               workspace_context: fixture.workspace_context,
               batch_id: "occurrence-race-claim:" <> fixture.workspace_id,
               owner_id: "scheduler-race",
               lease_duration_ms: 30_000,
               limit: 10
             })

    assert {:ok, _deactivated} =
             SchedulerStore.set_activation(%{
               activation
               | enabled: false,
                 reason: "stop before dispatch",
                 command_id: "schedule-race-deactivate:" <> fixture.workspace_id,
                 request_hash: :crypto.hash(:sha256, "race-deactivate:" <> fixture.workspace_id),
                 occurred_at: DateTime.utc_now(),
                 next_due_at: nil
             })

    assert {:ok, %{items: [%{status: :suppressed}]}} =
             SchedulerStore.page_occurrences(%PageScheduleOccurrences{
               workspace_context: fixture.workspace_context,
               pipeline_target_id: fixture.pipeline_target_id,
               schedule_id: "daily",
               limit: 10
             })

    assert {:error, %{kind: :fenced}} =
             SchedulerStore.authorize_occurrence_dispatch(%AuthorizeScheduleOccurrenceDispatch{
               workspace_context: fixture.workspace_context,
               command_id: "occurrence-race-dispatch:" <> fixture.workspace_id,
               submission: schedule_submission(fixture, "suppressed"),
               occurrence_id: claimed.occurrence_id,
               pipeline_target_id: fixture.pipeline_target_id,
               schedule_id: "daily",
               schedule_fingerprint: schedule.schedule_fingerprint,
               owner_id: claimed.claim_owner,
               claim_generation: claimed.claim_generation,
               occurred_at: DateTime.utc_now()
             })
  end

  test "serializes capacity admission and releases counters exactly once", fixture do
    {first_run_command, first_run} = create_run_command(fixture)
    {second_run_command, second_run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(first_run_command)
    assert {:ok, _created} = RunStore.create_run(second_run_command)

    first = admit_command(fixture, first_run.id, "first")
    second = admit_command(fixture, second_run.id, "second")

    assert {:ok, %{status: :admitted, lease: lease}} = AdmissionStore.admit(first)
    assert lease.scope_ids == [fixture.capacity_scope_id]
    assert {:ok, %{status: :admitted, lease: ^lease}} = AdmissionStore.admit(first)

    renewal = %RenewExecutionLease{
      workspace_context: fixture.workspace_context,
      renewal_id: "renew:" <> lease.lease_id,
      lease_id: lease.lease_id,
      owner_id: lease.owner_id,
      owner_generation: lease.owner_generation,
      lease_duration_ms: 30_000
    }

    assert {:ok, renewed} = AdmissionStore.renew_lease(renewal)
    assert {:ok, ^renewed} = AdmissionStore.renew_lease(renewal)

    assert {:ok, waiting} = AdmissionStore.admit(second)
    assert waiting.status == :waiting
    assert waiting.blocking_scope_id == fixture.capacity_scope_id
    assert {:ok, ^waiting} = AdmissionStore.admit(second)

    release = %ReleaseExecutionLease{
      workspace_context: fixture.workspace_context,
      lease_id: lease.lease_id,
      owner_id: lease.owner_id,
      owner_generation: lease.owner_generation
    }

    assert {:ok, released} = AdmissionStore.release_lease(release)
    assert released.freed_scope_ids == [fixture.capacity_scope_id]
    assert {:ok, ^released} = AdmissionStore.release_lease(release)

    retry_after_release = %{second | command_id: second.command_id <> ":retry"}
    assert {:ok, %{status: :admitted}} = AdmissionStore.admit(retry_after_release)
  end

  test "fences materialization claims and preserves an immutable success ledger", fixture do
    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    claim_command = %ClaimMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "materialization-claim:" <> run.id,
      claim_key: "claim:" <> run.id,
      deployment_id: fixture.deployment_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      evidence_generation_id:
        fixture.version.manifest.assets
        |> Enum.find(&(&1.ref == {MyApp.Asset, :asset}))
        |> Map.fetch!(:semantic_generation_id),
      partition_key: Favn.Freshness.Key.latest(),
      run_id: run.id,
      owner_id: "worker-a",
      lease_duration_ms: 30_000,
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, claimed} = MaterializationStore.claim(claim_command)
    assert claimed.status == :claimed
    assert claimed.claim.fencing_token == 1
    assert {:ok, ^claimed} = MaterializationStore.claim(claim_command)

    assert {:ok, competing} =
             MaterializationStore.claim(%{
               claim_command
               | command_id: claim_command.command_id <> ":other",
                 owner_id: "worker-b"
             })

    assert competing.status == :competing
    assert competing.claim.owner_id == "worker-a"

    renewal = %RenewMaterializationClaim{
      workspace_context: fixture.workspace_context,
      renewal_id: "materialization-renew:" <> run.id,
      claim_key: claim_command.claim_key,
      owner_id: "worker-a",
      fencing_token: claimed.claim.fencing_token,
      lease_duration_ms: 30_000
    }

    assert {:ok, renewed} = MaterializationStore.renew(renewal)
    assert {:ok, ^renewed} = MaterializationStore.renew(renewal)

    node_key_fingerprint =
      FavnOrchestrator.AssetStepIdentity.node_fingerprint({{MyApp.Asset, :asset}, nil})

    finish = %FinishMaterialization{
      workspace_context: fixture.workspace_context,
      command_id: "materialization-finish:" <> run.id,
      claim_key: claim_command.claim_key,
      owner_id: "worker-a",
      fencing_token: claimed.claim.fencing_token,
      expected_version: claimed.claim.version,
      status: :succeeded,
      materialization_id: "materialization:" <> run.id,
      payload: %{
        "row_count" => 42,
        "output_ref" => "warehouse/table",
        "run_id" => run.id,
        "freshness_version" => "#{run.id}:#{node_key_fingerprint}",
        "node_key_fingerprint" => node_key_fingerprint
      },
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, materialized} = MaterializationStore.finish(finish)
    assert materialized.status == :materialized
    assert materialized.materialization.materialization_id == finish.materialization_id
    assert {:ok, ^materialized} = MaterializationStore.finish(finish)
    assert {:ok, ^materialized} = MaterializationStore.claim(claim_command)

    assert {:ok, [^materialized, missing]} =
             MaterializationStore.get_many(%GetMaterializations{
               workspace_context: fixture.workspace_context,
               claim_keys: [claim_command.claim_key, "claim:missing"]
             })

    assert missing.status == :missing

    assert {:ok, _publications} = Sequencer.sequence_batch()

    count = drain_projector("projector:" <> run.id)
    assert count > 0

    assert %{rows: [[persisted_node_key_hash]]} =
             SQL.query!(
               Repo,
               "SELECT latest_success_node_key_hash FROM favn_control.asset_freshness_states WHERE workspace_id = $1 AND latest_success_materialization_id = $2",
               [fixture.workspace_id, finish.materialization_id]
             )

    assert persisted_node_key_hash == Base.decode16!(node_key_fingerprint, case: :mixed)

    assert {:ok, detail} =
             Catalogue.active_asset_detail(fixture.workspace_context, fixture.target_id,
               now: DateTime.utc_now()
             )

    assert %{state: :fresh} = detail.freshness
    assert detail.freshness.latest_success.run_id == run.id
  end

  test "redacts, deduplicates, pages, and purges bounded log batches", fixture do
    now = DateTime.utc_now()

    command = %AppendLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "logs:" <> fixture.workspace_id,
      batch_id: "log-batch:" <> fixture.workspace_id,
      occurred_at: now,
      entries: [
        %LogEntry{
          source: "runner",
          level: :error,
          message: "request failed token=super-secret-value",
          metadata: %{"password" => "not-stored", "attempt" => 1},
          occurred_at: now
        },
        %LogEntry{
          source: "scheduler",
          level: :info,
          message: "schedule evaluated",
          metadata: %{},
          occurred_at: DateTime.add(now, -1, :second)
        }
      ]
    }

    assert {:ok, entries} = LogStore.append_batch(command)
    assert length(entries) == 2
    refute hd(entries).message =~ "super-secret-value"
    assert hd(entries).metadata["password"] == "[REDACTED]"
    assert {:ok, ^entries} = LogStore.append_batch(command)

    assert {:ok, page} =
             LogStore.page(%PageLogs{
               workspace_context: fixture.workspace_context,
               filter: %Favn.Log.Filter{levels: [:error]} |> Map.from_struct(),
               limit: 10
             })

    assert [error_entry] = page.items
    assert error_entry.level == :error

    assert {:ok, purged} =
             LogStore.purge(%PurgeLogs{
               workspace_context: fixture.workspace_context,
               cutoff: DateTime.add(now, 1, :second),
               limit: 10
             })

    assert purged.deleted_count == 2
  end

  test "normalizes runner metadata before validating the persisted payload", fixture do
    now = DateTime.utc_now()
    asset_ref = {__MODULE__.PipelineAsset, :asset}

    command = %AppendLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "runner-log-normalized:" <> fixture.workspace_id,
      batch_id: "runner-log-normalized-batch:" <> fixture.workspace_id,
      occurred_at: now,
      entries: [
        %LogEntry{
          source: "runner",
          level: :info,
          message: "asset execution started",
          metadata: %{
            pipeline_identity_ref: {__MODULE__.Pipeline, :pipeline},
            pipeline_target_refs: [asset_ref]
          },
          occurred_at: now
        }
      ]
    }

    assert {:ok, [entry]} = LogStore.append_batch(command)

    assert entry.metadata["pipeline_identity_ref"] == %{
             "module" => Atom.to_string(__MODULE__.Pipeline),
             "name" => "pipeline"
           }

    assert entry.metadata["pipeline_target_refs"] == [
             %{"module" => Atom.to_string(elem(asset_ref, 0)), "name" => "asset"}
           ]
  end

  test "rejects runner metadata that remains oversized after normalization", fixture do
    now = DateTime.utc_now()

    metadata =
      Map.new(1..5, fn index ->
        {"detail_#{index}", String.duplicate(Integer.to_string(index), 8_192)}
      end)

    command = %AppendLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "runner-log-oversized:" <> fixture.workspace_id,
      batch_id: "runner-log-oversized-batch:" <> fixture.workspace_id,
      occurred_at: now,
      entries: [
        %LogEntry{
          source: "runner",
          level: :info,
          message: "asset execution started",
          metadata: metadata,
          occurred_at: now
        }
      ]
    }

    assert {:error, %Error{kind: :invalid, message: "invalid persistence command"}} =
             LogStore.append_batch(command)
  end

  test "rejects malformed runner metadata as an invalid command", fixture do
    now = DateTime.utc_now()

    command = %AppendLogBatch{
      workspace_context: fixture.workspace_context,
      command_id: "runner-log-malformed:" <> fixture.workspace_id,
      batch_id: "runner-log-malformed-batch:" <> fixture.workspace_id,
      occurred_at: now,
      entries: [
        %LogEntry{
          source: "runner",
          level: :info,
          message: "asset execution started",
          metadata: %{"malformed" => [1 | 2]},
          occurred_at: now
        }
      ]
    }

    assert {:error, %Error{kind: :invalid, message: "invalid persistence command"}} =
             LogStore.append_batch(command)
  end

  test "persists normalized actors, hashed sessions, access audit, and revocation", fixture do
    now = DateTime.utc_now()
    actor_id = "actor:" <> fixture.workspace_id
    password_hash = "$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaC1vbmU"

    create = %CreateActor{
      workspace_context: fixture.workspace_context,
      command_id: "actor-create:" <> actor_id,
      actor_id: actor_id,
      username: "  User-#{fixture.workspace_id}@Example.COM  ",
      display_name: "Workspace Operator",
      password_hash: password_hash,
      roles: [:customer_operator],
      occurred_at: now
    }

    assert {:ok, actor} = IdentityStore.create_actor(create)
    assert actor.actor_id == actor_id
    assert actor.roles == [:customer_operator]
    assert actor.credential_hash == password_hash
    assert {:ok, ^actor} = IdentityStore.create_actor(create)

    assert {:ok, fetched} =
             IdentityStore.get_actor(%GetActor{
               workspace_context: fixture.workspace_context,
               selector: %ActorByUsername{username: String.downcase(String.trim(create.username))}
             })

    assert fetched.actor_id == actor_id

    token_hash = :crypto.hash(:sha256, "session-token:" <> actor_id)

    session_command = %CreateSession{
      workspace_context: fixture.workspace_context,
      command_id: "session-create:" <> actor_id,
      session_id: "session:" <> actor_id,
      actor_id: actor_id,
      token_hash: token_hash,
      provider: "password_local",
      expected_credential_version: fetched.credential_version,
      expires_at: DateTime.add(now, 3_600, :second),
      occurred_at: now
    }

    assert {:ok, session} = IdentityStore.create_session(session_command)
    assert session.status == :active
    assert {:ok, ^session} = IdentityStore.create_session(session_command)

    assert {:ok, ^session} =
             IdentityStore.get_session(%GetSession{
               workspace_context: fixture.workspace_context,
               selector: %SessionByTokenHash{token_hash: token_hash}
             })

    changed_hash = "$argon2id$v=19$m=65536,t=3,p=4$c2FsdDI$aGFzaC10d28"

    assert :ok =
             IdentityStore.change_password(%ChangeActorPassword{
               workspace_context: fixture.workspace_context,
               command_id: "password-change:" <> actor_id,
               actor_id: actor_id,
               password_hash: changed_hash,
               expected_credential_version: 1,
               occurred_at: now,
               revoke_sessions?: true
             })

    assert {:ok, revoked} =
             IdentityStore.get_session(%GetSession{
               workspace_context: fixture.workspace_context,
               selector: %SessionByTokenHash{token_hash: token_hash}
             })

    assert revoked.status == :revoked

    assert :ok =
             IdentityStore.revoke_sessions(%RevokeSessions{
               workspace_context: fixture.workspace_context,
               command_id: "session-revoke:" <> actor_id,
               session_id: session.session_id,
               occurred_at: now
             })

    assert {:ok, audit_page} =
             IdentityStore.page_audit(%PageAudit{
               scope: fixture.workspace_context,
               limit: 20
             })

    assert Enum.any?(audit_page.items, &(&1.action == "actor.created"))
    assert Enum.any?(audit_page.items, &(&1.action == "actor.password.changed"))
  end

  test "workspace identity use cases authenticate, page, update, and revoke", fixture do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "operator-#{suffix}"
    password = "operator-password-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               password,
               "Workspace Operator",
               [:operator]
             )

    assert actor.workspace_id == fixture.workspace_id
    assert actor.roles == [:operator]
    refute Map.has_key?(actor, :credential_hash)

    assert {:ok, login_context} =
             WorkspaceContext.new(fixture.workspace_id, "auth:password", [:customer_reader])

    assert {:ok, authenticated} =
             Identity.authenticate_password(login_context, username, password)

    assert authenticated.id == actor.id

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(login_context, username, "not-the-password")

    assert {:error, :forbidden} =
             Identity.change_password(
               fixture.workspace_context,
               actor.id,
               password,
               "replacement-password"
             )

    assert {:ok, self_context} =
             WorkspaceContext.new(fixture.workspace_id, actor.id, [:customer_operator])

    assert {:error, :invalid_current_password} =
             Identity.change_password(
               self_context,
               actor.id,
               "wrong-current-password",
               "replacement-password"
             )

    assert {:ok, _still_authenticated} =
             Identity.authenticate_password(login_context, username, password)

    assert :ok =
             Identity.change_password(
               self_context,
               actor.id,
               password,
               "replacement-password"
             )

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(login_context, username, password)

    assert {:ok, authenticated} =
             Identity.authenticate_password(login_context, username, "replacement-password")

    assert {:ok, issued} =
             Identity.issue_session(login_context, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    assert is_binary(issued.token)
    refute Map.has_key?(issued, :token_hash)

    assert {:ok, introspected, active_actor} =
             Identity.introspect_session(login_context, issued.token)

    assert introspected.id == issued.id
    assert active_actor.id == actor.id

    assert {:ok, page} = Identity.page_actors(fixture.workspace_context, limit: 10)
    assert Enum.any?(page.items, &(&1.id == actor.id))

    assert {:ok, admin_actor} =
             Identity.set_roles(
               fixture.workspace_context,
               actor.id,
               [:admin],
               actor.access_version
             )

    assert admin_actor.roles == [:admin]

    assert {:error, %{kind: :conflict}} =
             Identity.set_roles(
               fixture.workspace_context,
               actor.id,
               [:viewer],
               actor.access_version
             )

    assert :ok = Identity.revoke_session(fixture.workspace_context, issued.id)

    assert {:error, :invalid_session} =
             Identity.introspect_session(login_context, issued.token)
  end

  test "sessions stay tenant-bound and workspace switching rotates authority atomically",
       fixture do
    target = provision_deploy_fixture(fixture.version)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "switch-#{suffix}"
    password = "workspace-switch-password-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               password,
               "Workspace Switcher",
               [:admin]
             )

    assert {:ok, _target_actor} =
             Identity.attach_actor_membership(target.workspace_context, username, [:operator])

    {:ok, source_login} =
      WorkspaceContext.new(fixture.workspace_id, "auth:source", [:customer_reader])

    {:ok, target_login} =
      WorkspaceContext.new(target.workspace_id, "auth:target", [:customer_reader])

    assert {:ok, authenticated} =
             Identity.authenticate_password(source_login, username, password)

    assert {:ok, source_session} =
             Identity.issue_session(source_login, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    assert {:error, :invalid_session} =
             Identity.introspect_session(target_login, source_session.token)

    {:ok, self_context} =
      WorkspaceContext.new(fixture.workspace_id, actor.id, [:workspace_admin],
        request_id: source_session.id
      )

    assert {:ok, memberships} = Identity.list_actor_memberships(self_context, actor.id)

    assert Enum.map(memberships, & &1.id) |> Enum.sort() ==
             Enum.sort([fixture.workspace_id, target.workspace_id])

    assert {:error, :forbidden} =
             Identity.list_actor_memberships(fixture.workspace_context, actor.id)

    assert {:ok, target_session} =
             Identity.rotate_workspace_session(
               self_context,
               source_session,
               target.workspace_id
             )

    assert target_session.workspace_id == target.workspace_id

    assert {:error, :invalid_session} =
             Identity.introspect_session(source_login, source_session.token)

    assert {:ok, introspected, switched_actor} =
             Identity.introspect_session(target_login, target_session.token)

    assert introspected.id == target_session.id
    assert switched_actor.id == actor.id

    assert {:error, %Error{kind: :not_found}} =
             IdentityStore.get_session(%GetSession{
               workspace_context: fixture.workspace_context,
               selector: %FavnOrchestrator.Persistence.Selectors.SessionById{
                 session_id: target_session.id
               }
             })

    assert {:ok, source_sessions} =
             IdentityStore.page_sessions(%PageSessions{
               workspace_context: fixture.workspace_context,
               actor_id: actor.id,
               limit: 10
             })

    assert Enum.map(source_sessions.items, &{&1.session_id, &1.status}) == [
             {source_session.id, :revoked}
           ]

    assert {:ok, target_sessions} =
             IdentityStore.page_sessions(%PageSessions{
               workspace_context: target.workspace_context,
               actor_id: actor.id,
               limit: 10
             })

    assert Enum.map(target_sessions.items, & &1.session_id) == [target_session.id]

    assert {:ok, source_audit} =
             IdentityStore.page_audit(%PageAudit{
               scope: fixture.workspace_context,
               limit: 20
             })

    assert {:ok, target_audit} =
             IdentityStore.page_audit(%PageAudit{
               scope: target.workspace_context,
               limit: 20
             })

    assert Enum.any?(source_audit.items, &(&1.action == "session.workspace_switched"))
    assert Enum.any?(target_audit.items, &(&1.action == "session.workspace_switched"))
  end

  test "membership suspension is workspace-only and global disable is platform-only", fixture do
    target = provision_deploy_fixture(fixture.version)
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "security-#{suffix}"
    password = "security-password-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               password,
               "Security Actor",
               [:admin]
             )

    assert {:ok, target_actor} =
             Identity.set_membership(target.workspace_context, actor.id, [:operator], 0)

    {:ok, source_login} =
      WorkspaceContext.new(fixture.workspace_id, "auth:source", [:customer_reader])

    {:ok, target_login} =
      WorkspaceContext.new(target.workspace_id, "auth:target", [:customer_reader])

    assert {:ok, authenticated} =
             Identity.authenticate_password(source_login, username, password)

    assert {:ok, source_session} =
             Identity.issue_session(source_login, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    assert {:ok, target_session} =
             Identity.issue_session(target_login, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    assert {:ok, suspended} =
             Identity.set_membership_access(
               target.workspace_context,
               actor.id,
               [:operator],
               :suspended,
               target_actor.access_version
             )

    assert suspended.status == :disabled
    assert {:ok, _, _} = Identity.introspect_session(source_login, source_session.token)

    assert {:error, :invalid_session} =
             Identity.introspect_session(target_login, target_session.token)

    assert {:error, %Error{kind: :conflict}} =
             Identity.set_membership_access(
               fixture.workspace_context,
               actor.id,
               [:viewer],
               :active,
               actor.access_version
             )

    assert {:ok, disabled} =
             AdminLifecycle.set_actor_status(username, :disabled, identity_store: IdentityStore)

    assert disabled.actor_id == actor.id
    assert disabled.sessions_revoked

    assert {:ok, %{status: :disabled}} =
             AdminLifecycle.set_actor_status(username, :disabled, identity_store: IdentityStore)

    assert {:ok, %{status: :disabled}} =
             IdentityStore.get_global_actor(%GetGlobalActor{
               platform_context: fixture.platform_context,
               username: username
             })

    assert {:ok, disabled_actor_page} =
             Identity.page_actors(fixture.workspace_context, limit: 100)

    disabled_actor = Enum.find(disabled_actor_page.items, &(&1.id == actor.id))
    assert disabled_actor.status == :disabled
    assert disabled_actor.global_status == :disabled

    assert {:error, :invalid_session} =
             Identity.introspect_session(source_login, source_session.token)

    assert {:ok, %{status: :active, sessions_revoked: false}} =
             AdminLifecycle.set_actor_status(username, :active, identity_store: IdentityStore)

    assert {:ok, _authenticated_again} =
             Identity.authenticate_password(source_login, username, password)

    assert {:ok, current_global_actor} =
             IdentityStore.get_global_actor(%GetGlobalActor{
               platform_context: fixture.platform_context,
               username: username
             })

    assert {:error, %Error{kind: :conflict}} =
             IdentityStore.set_actor_status(%SetActorStatus{
               platform_context: fixture.platform_context,
               command_id: "actor-status-stale:#{actor.id}",
               actor_id: actor.id,
               status: :disabled,
               expected_version: current_global_actor.version - 1,
               occurred_at: DateTime.utc_now()
             })

    assert {:ok, platform_audit} =
             IdentityStore.page_audit(%PageAudit{
               scope: fixture.platform_context,
               limit: 20
             })

    assert Enum.any?(platform_audit.items, fn entry ->
             entry.action == "actor.status.changed" and entry.subject_id == actor.id
           end)
  end

  test "Entra identities are explicitly linked and Favn authorization remains authoritative",
       fixture do
    tenant_id = "11111111-1111-4111-8111-111111111111"
    object_id = "22222222-2222-4222-8222-222222222222"
    username = "entra-#{System.unique_integer([:positive])}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               "entra-password-long",
               "Entra Actor",
               [:viewer]
             )

    assert {:ok, linked} =
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               object_id,
               :link,
               identity_store: IdentityStore
             )

    assert linked.actor_id == actor.id

    {:ok, login_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:external-login", [:customer_reader])

    identity = %{
      provider: "azure_container_apps_entra",
      tenant_id: tenant_id,
      subject_id: object_id
    }

    assert {:ok, external_actor} =
             Identity.authenticate_external_identity(login_context, identity)

    assert external_actor.id == actor.id
    assert external_actor.roles == [:viewer]

    second_username = "entra-second-#{System.unique_integer([:positive])}"

    assert {:ok, _second_actor} =
             Identity.create_actor(
               fixture.workspace_context,
               second_username,
               "entra-second-password-long",
               "Second Entra Actor",
               [:viewer]
             )

    assert {:error, %Error{kind: :conflict}} =
             AdminLifecycle.configure_entra_identity(
               second_username,
               tenant_id,
               object_id,
               :link,
               identity_store: IdentityStore
             )

    assert {:error,
            %Error{
              kind: :constraint,
              details: %{constraint: "auth_external_identities_actor_uidx"}
            }} =
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               "33333333-3333-4333-8333-333333333333",
               :link,
               identity_store: IdentityStore
             )

    parent = self()

    stale_login =
      Task.async(fn ->
        receive do
          :start -> :ok
        end

        {:ok, stale_actor} = Identity.authenticate_external_identity(login_context, identity)
        send(parent, :external_identity_resolved)

        receive do
          :continue -> :ok
        end

        Identity.issue_session(login_context, stale_actor.id,
          provider: "azure_container_apps_entra",
          external_tenant_id: tenant_id,
          external_subject_id: object_id
        )
      end)

    Sandbox.allow(Repo, self(), stale_login.pid)
    send(stale_login.pid, :start)
    assert_receive :external_identity_resolved

    assert {:ok, _unlinked_during_login} =
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               object_id,
               :unlink,
               identity_store: IdentityStore
             )

    send(stale_login.pid, :continue)
    assert {:error, %Error{kind: :forbidden}} = Task.await(stale_login)

    assert {:ok, _relinked} =
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               object_id,
               :link,
               identity_store: IdentityStore
             )

    assert {:ok, session} =
             Identity.issue_session(login_context, actor.id,
               provider: "azure_container_apps_entra",
               external_tenant_id: tenant_id,
               external_subject_id: object_id
             )

    assert session.provider == "azure_container_apps_entra"

    assert {:ok, _session, %{id: actor_id}} =
             Identity.introspect_session(login_context, session.token)

    assert actor_id == actor.id

    assert {:ok, platform_audit} =
             IdentityStore.page_audit(%PageAudit{
               scope: fixture.platform_context,
               limit: 50
             })

    link_audit =
      Enum.find(platform_audit.items, fn entry ->
        entry.action == "external_identity.linked" and entry.subject_id == actor.id
      end)

    assert link_audit
    refute inspect(link_audit) =~ object_id
    refute inspect(link_audit) =~ tenant_id

    assert {:ok, _unlinked} =
             AdminLifecycle.configure_entra_identity(
               username,
               tenant_id,
               object_id,
               :unlink,
               identity_store: IdentityStore
             )

    assert {:error, :invalid_credentials} =
             Identity.authenticate_external_identity(login_context, identity)

    assert {:error, :invalid_session} =
             Identity.introspect_session(login_context, session.token)

    assert {:error, :invalid_credentials} = Auth.external_login(login_context, identity)

    assert {:ok, workspace_audit} =
             IdentityStore.page_audit(%PageAudit{
               scope: fixture.workspace_context,
               limit: 50
             })

    denial =
      Enum.find(workspace_audit.items, fn entry ->
        entry.action == "external_login.denied"
      end)

    assert denial
    refute inspect(denial) =~ object_id
    refute inspect(denial) =~ tenant_id

    assert {:error, %Error{kind: :not_found}} =
             IdentityStore.get_actor(%GetActor{
               workspace_context: login_context,
               selector: %ActorByExternalIdentity{
                 provider: "azure_container_apps_entra",
                 tenant_id: tenant_id,
                 subject_id: object_id
               }
             })
  end

  test "operator administration facades reauthorize reads and self password rotation", fixture do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "facade-admin-#{suffix}"
    password = "facade-admin-password-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               password,
               "Facade Administrator",
               [:admin]
             )

    {:ok, login_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:facade", [:customer_reader])

    assert {:ok, authenticated} =
             Identity.authenticate_password(login_context, username, password)

    assert {:ok, session} =
             Identity.issue_session(login_context, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    assert {:ok, operator_context} =
             FavnOrchestrator.operator_context(fixture.workspace_id, actor, session)

    assert {:ok, actor_page} =
             FavnOrchestrator.page_operator_actors(operator_context, limit: 10)

    assert Enum.any?(actor_page.items, &(&1.id == actor.id))

    assert {:ok, session_page} =
             FavnOrchestrator.page_operator_sessions(operator_context, limit: 10)

    assert Enum.any?(session_page.items, &(&1.id == session.id))

    assert {:ok, audit_page} =
             FavnOrchestrator.page_operator_audit(operator_context, limit: 10)

    assert Enum.any?(audit_page.items, &(&1.action == "session.created"))

    replacement_password = "facade-replacement-password-#{suffix}"

    assert :ok =
             FavnOrchestrator.change_operator_password(
               operator_context,
               password,
               replacement_password
             )

    assert {:error, :invalid_session} =
             Identity.introspect_session(login_context, session.token)

    assert {:ok, _authenticated_again} =
             Identity.authenticate_password(login_context, username, replacement_password)
  end

  test "operational API audit is durable and workspace scoped", fixture do
    assert :ok =
             Identity.record_audit(fixture.workspace_context, %{
               action: "api.run.submitted",
               resource_type: "run",
               resource_id: "run-audit-#{fixture.workspace_id}",
               status: "accepted",
               password: "must-be-redacted"
             })

    assert {:ok, page} = Identity.page_audit(fixture.workspace_context, limit: 20)
    entry = Enum.find(page.items, &(&1.action == "api.run.submitted"))

    assert entry.subject_kind == "run"
    assert entry.subject_id == "run-audit-#{fixture.workspace_id}"
    assert entry.detail["password"] == "[REDACTED]"
  end

  test "operator audit replay requires the exact same redacted command", fixture do
    identity = api_identity(fixture, [:operator])

    entry = %{
      action: "run.cancel",
      actor_id: identity.actor.id,
      session_id: identity.session.id,
      resource_type: "run",
      resource_id: "run-audit-replay",
      service_identity: "same_beam_operator_ui",
      outcome: "requested",
      detail: %{request: %{run_id: "run-audit-replay"}},
      idempotency: %{
        operation: "run.cancel",
        key_hash:
          :crypto.hash(:sha256, "operator-audit-replay-key")
          |> Base.encode16(case: :lower),
        replayed: false
      }
    }

    assert :ok = Identity.record_audit(fixture.workspace_context, entry)
    assert :ok = Identity.record_audit(fixture.workspace_context, entry)

    assert {:error, %Error{kind: :conflict}} =
             Identity.record_audit(
               fixture.workspace_context,
               put_in(entry, [:detail, :request, :run_id], "changed-run")
             )
  end

  test "pending operator commands survive reconnect and become terminal", fixture do
    identity = api_identity(fixture, [:operator])

    assert {:ok, lookup_context} =
             WorkspaceContext.new(fixture.workspace_id, "browser:test", [:customer_reader])

    assert {:ok, context, session, actor} =
             Identity.authorize_session(
               lookup_context,
               identity.actor.id,
               identity.session.id,
               :operator
             )

    assert {:ok, operator_context} =
             OperatorContext.new(fixture.workspace_id, actor, session)

    request = %{run_id: "run-reconnect", metadata: %{password: "never-fingerprint-me"}}

    assert {:ok, first} =
             OperatorAudit.begin_command(
               context,
               operator_context,
               actor,
               "run.cancel",
               "run",
               "run-reconnect",
               request,
               "run-cancel:first:0123456789abcdef"
             )

    expired_at = DateTime.add(DateTime.utc_now(), -3_600, :second)
    cutoff = DateTime.utc_now()

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.idempotency_records (
        workspace_id, operation, principal_kind, principal_id, key_hash,
        request_fingerprint, status, expires_at, inserted_at, updated_at
      )
      VALUES ($1, $2, 'actor', $3, $4, $5, 'started', $6, $6, $6)
      """,
      [
        fixture.workspace_id,
        first.operation,
        actor.id,
        first.key_hash,
        first.request_fingerprint,
        expired_at
      ]
    )

    assert {:ok, %{status: :completed, batch_count: 0}} =
             MaintenanceStore.purge(%PurgePersistence{
               platform_context: fixture.platform_context,
               job_id: "retain-operator-idempotency-#{System.unique_integer([:positive])}",
               target: :idempotency,
               workspace_id: fixture.workspace_id,
               cutoff: cutoff,
               limit: 10
             })

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.idempotency_records
               WHERE workspace_id = $1 AND operation = $2 AND key_hash = $3
               """,
               [fixture.workspace_id, first.operation, first.key_hash]
             )

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.auth_sessions
      SET status = 'revoked', expires_at = $3, updated_at = $3
      WHERE workspace_id = $1 AND session_id = $2
      """,
      [fixture.workspace_id, session.id, expired_at]
    )

    assert {:ok, %{status: :completed, batch_count: 0}} =
             MaintenanceStore.purge(%PurgePersistence{
               platform_context: fixture.platform_context,
               job_id: "retain-operator-session-#{System.unique_integer([:positive])}",
               target: :sessions,
               workspace_id: fixture.workspace_id,
               cutoff: cutoff,
               limit: 10
             })

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.auth_sessions
               WHERE workspace_id = $1 AND session_id = $2
               """,
               [fixture.workspace_id, session.id]
             )

    assert {:ok, replacement_session} =
             Identity.issue_session(lookup_context, actor.id,
               expected_credential_version: identity.credential_version
             )

    assert {:ok, replacement_context, replacement_session, replacement_actor} =
             Identity.authorize_session(
               lookup_context,
               actor.id,
               replacement_session.id,
               :operator
             )

    assert {:ok, replacement_operator_context} =
             OperatorContext.new(
               fixture.workspace_id,
               replacement_actor,
               replacement_session
             )

    assert {:ok, recovered} =
             OperatorAudit.begin_command(
               replacement_context,
               replacement_operator_context,
               replacement_actor,
               "run.cancel",
               "run",
               "run-reconnect",
               request,
               "run-cancel:first:0123456789abcdef"
             )

    assert recovered.key_hash == first.key_hash
    assert recovered.request_fingerprint == first.request_fingerprint

    assert %{rows: [[replacement_session_id]]} =
             SQL.query!(
               Repo,
               """
               SELECT session_id
               FROM favn_control.auth_operator_commands
               WHERE workspace_id = $1 AND key_hash = $2
               """,
               [fixture.workspace_id, recovered.key_hash]
             )

    assert replacement_session_id == replacement_session.id

    assert %{rows: [[true]]} =
             SQL.query!(
               Repo,
               """
               SELECT expires_at > $4
               FROM favn_control.idempotency_records
               WHERE workspace_id = $1 AND operation = $2 AND key_hash = $3
               """,
               [fixture.workspace_id, first.operation, first.key_hash, cutoff]
             )

    assert {:error, %Error{kind: :conflict, retryable?: true}} =
             OperatorAudit.begin_command(
               replacement_context,
               replacement_operator_context,
               replacement_actor,
               "run.cancel",
               "run",
               "run-reconnect",
               request,
               "run-cancel:second:0123456789abcdef"
             )

    assert {:error, %Error{kind: :conflict, retryable?: true}} =
             OperatorAudit.begin_command(
               replacement_context,
               replacement_operator_context,
               replacement_actor,
               "run.cancel",
               "run",
               "run-reconnect",
               %{run_id: "changed-run"},
               "run-cancel:first:0123456789abcdef"
             )

    assert {:error, %Error{kind: :conflict, retryable?: true}} =
             OperatorAudit.begin_command(
               replacement_context,
               replacement_operator_context,
               replacement_actor,
               "run.cancel",
               "run",
               "run-reconnect",
               put_in(request, [:metadata, :password], "different-secret"),
               "run-cancel:first:0123456789abcdef"
             )

    assert :ok =
             OperatorAudit.finish_command(
               replacement_context,
               replacement_operator_context,
               replacement_actor,
               recovered,
               "accepted",
               "run",
               "run-reconnect",
               %{run_id: "run-reconnect"}
             )

    assert {:ok, next} =
             OperatorAudit.begin_command(
               context,
               operator_context,
               actor,
               "run.cancel",
               "run",
               "run-reconnect",
               request,
               "run-cancel:third:0123456789abcdef"
             )

    refute next.key_hash == first.key_hash

    assert {:ok, page} = Identity.page_audit(fixture.workspace_context, limit: 20)

    outcomes =
      page.items
      |> Enum.filter(&(&1.action == "run.cancel"))
      |> Enum.map(& &1.detail["outcome"])

    assert "requested" in outcomes
    assert "accepted" in outcomes
    assert Enum.any?(page.items, &(&1.action == "run.cancel.recovered"))
    refute inspect(page.items) =~ "never-fingerprint-me"
  end

  test "an unknown operator command can resolve to a proven terminal result", fixture do
    identity = api_identity(fixture, [:operator])

    assert {:ok, lookup_context} =
             WorkspaceContext.new(fixture.workspace_id, "browser:test", [:customer_reader])

    assert {:ok, context, session, actor} =
             Identity.authorize_session(
               lookup_context,
               identity.actor.id,
               identity.session.id,
               :operator
             )

    assert {:ok, operator_context} =
             OperatorContext.new(fixture.workspace_id, actor, session)

    request = %{run_id: "run-unknown"}
    key = "run-cancel:unknown:0123456789abcdef"

    assert {:ok, intent} =
             OperatorAudit.begin_command(
               context,
               operator_context,
               actor,
               "run.cancel",
               "run",
               "run-unknown",
               request,
               key
             )

    assert :ok =
             OperatorAudit.finish_command(
               context,
               operator_context,
               actor,
               intent,
               "unknown",
               "run",
               "run-unknown",
               %{reason: "transport_outcome_unknown"}
             )

    assert {:error, %Error{kind: :conflict, retryable?: true}} =
             OperatorAudit.begin_command(
               context,
               operator_context,
               actor,
               "run.cancel",
               "run",
               "run-unknown",
               request,
               "run-cancel:new-after-unknown:0123456789abcdef"
             )

    assert :ok =
             OperatorAudit.finish_command(
               context,
               operator_context,
               actor,
               intent,
               "accepted",
               "run",
               "run-unknown",
               %{run_id: "run-unknown"}
             )

    assert {:ok, page} = Identity.page_audit(fixture.workspace_context, limit: 20)

    outcomes =
      page.items
      |> Enum.filter(&(&1.action == "run.cancel"))
      |> Enum.map(& &1.detail["outcome"])

    assert "unknown" in outcomes
    assert "accepted" in outcomes
  end

  test "service-token manifest activation is idempotent and durably audited", fixture do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])

    body = %{
      "selection" => %{
        "common_assets" => "all",
        "common_pipelines" => "all",
        "workspace_assets" => [],
        "workspace_pipelines" => []
      },
      "configuration" => %{
        "secret_store_url" => "https://activation.vault.example.test",
        "resources" => %{"ducklake" => %{"secret_ref" => "ducklake-metadata"}}
      }
    }

    path =
      "/api/orchestrator/v1/manifests/#{fixture.version.manifest_version_id}/activate"

    first =
      api_request(:post, path, body,
        fixture: fixture,
        idempotency_key: "activate-once"
      )

    replay =
      api_request(:post, path, body,
        fixture: fixture,
        idempotency_key: "activate-once"
      )

    assert first.status == 200
    assert replay.status == 200
    assert replay.resp_body == first.resp_body

    assert %{
             "data" => %{
               "runner_releases" => runner_releases
             }
           } = JSON.decode!(first.resp_body)

    assert runner_releases == fixture.version.runner_releases

    assert {:ok, %{targets: targets}} = Manifests.active(fixture.workspace_context)

    assert Enum.all?(targets.assets ++ targets.pipelines, fn target ->
             is_binary(target.label) and is_binary(target.target_id)
           end)

    assert {:ok, audit_page} = Identity.page_audit(fixture.workspace_context, limit: 20)

    matching_audits =
      Enum.filter(
        audit_page.items,
        &(&1.action == "manifest.activate" and
            &1.subject_id == fixture.version.manifest_version_id)
      )

    assert length(matching_audits) == 1

    assert hd(matching_audits).detail["runner_releases"] == fixture.version.runner_releases

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.idempotency_records WHERE workspace_id = $1 AND operation = 'manifest.activate'",
               [fixture.workspace_id]
             )
  end

  test "HTTP manifest activation returns not found for a missing staged manifest", fixture do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])

    response =
      api_request(
        :post,
        "/api/orchestrator/v1/manifests/mv_missing/activate",
        activation_body(),
        fixture: fixture,
        identity: api_identity(fixture, [:admin]),
        idempotency_key: "activate-missing-manifest"
      )

    assert response.status == 404
    assert %{"error" => %{"code" => "not_found"}} = JSON.decode!(response.resp_body)
  end

  test "HTTP manifest activation is independent of live runner availability",
       fixture do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :orchestrator, :manifest_activation_rejected],
        fn _event, measurements, metadata, pid ->
          send(pid, {:manifest_activation_rejected, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])

    response =
      api_request(
        :post,
        "/api/orchestrator/v1/manifests/#{fixture.version.manifest_version_id}/activate",
        activation_body(),
        fixture: fixture,
        identity: api_identity(fixture, [:admin]),
        idempotency_key: "activate-wrong-runner"
      )

    assert response.status == 200
    refute_receive {:manifest_activation_rejected, _measurements, _metadata}

    assert {:ok, %{manifest: active}} = Manifests.active(fixture.workspace_context)
    assert active.manifest_version_id == fixture.version.manifest_version_id
    assert active.runner_releases == fixture.version.runner_releases
  end

  test "HTTP manifest activation succeeds with zero live runners", fixture do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])

    response =
      api_request(
        :post,
        "/api/orchestrator/v1/manifests/#{fixture.version.manifest_version_id}/activate",
        activation_body(),
        fixture: fixture,
        identity: api_identity(fixture, [:admin]),
        idempotency_key: "activate-runner-offline"
      )

    assert response.status == 200

    assert %{"data" => %{"manifest_version_id" => manifest_version_id}} =
             JSON.decode!(response.resp_body)

    assert manifest_version_id == fixture.version.manifest_version_id
  end

  test "HTTP boundaries reject cross-workspace access and isolate SSE replay", fixture do
    other_fixture = provision_deploy_fixture(fixture.version)
    other_identity = api_identity(other_fixture, [:viewer])

    {first_command, first_run} = create_run_command(fixture)
    {other_command, other_run} = create_run_command(other_fixture)
    assert {:ok, _created} = RunStore.create_run(first_command)
    assert {:ok, _created} = RunStore.create_run(other_command)
    assert {:ok, _publications} = Sequencer.sequence_batch()

    cross_workspace =
      api_request(:get, "/api/orchestrator/v1/runs/#{first_run.id}", nil,
        fixture: other_fixture,
        identity: other_identity
      )

    assert cross_workspace.status == 404

    own_identity = api_identity(fixture, [:viewer])

    stream =
      api_request(:get, "/api/orchestrator/v1/streams/runs", nil,
        fixture: fixture,
        identity: own_identity
      )

    assert stream.status == 200
    assert stream.resp_body =~ first_run.id
    refute stream.resp_body =~ other_run.id
  end

  test "HTTP run detail distinguishes missing runs from unreadable snapshots", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    identity = api_identity(fixture, [:viewer])

    available =
      api_request(:get, "/api/orchestrator/v1/runs/#{run.id}", nil,
        fixture: fixture,
        identity: identity
      )

    assert available.status == 200

    assert %{
             "data" => %{
               "run" => %{"runner_releases" => runner_releases}
             }
           } = JSON.decode!(available.resp_body)

    assert runner_releases == fixture.version.runner_releases

    missing =
      api_request(:get, "/api/orchestrator/v1/runs/run_missing", nil,
        fixture: fixture,
        identity: identity
      )

    assert missing.status == 404
    assert %{"error" => %{"code" => "not_found"}} = JSON.decode!(missing.resp_body)

    SQL.query!(
      Repo,
      "UPDATE favn_control.runs SET snapshot = jsonb_build_object('garbage', true) WHERE workspace_id = $1 AND run_id = $2",
      [fixture.workspace_id, run.id]
    )

    unreadable =
      api_request(:get, "/api/orchestrator/v1/runs/#{run.id}", nil,
        fixture: fixture,
        identity: identity
      )

    assert unreadable.status == 500
    assert %{"error" => %{"code" => "run_unavailable"}} = JSON.decode!(unreadable.resp_body)
  end

  test "HTTP boundaries reject hidden targets and unscoped platform service tokens", fixture do
    identity = api_identity(fixture, [:operator])

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.workspace_deployment_targets
      SET customer_visible = false
      WHERE workspace_id = $1 AND deployment_id = $2
        AND target_kind = 'asset' AND target_id = $3
      """,
      [fixture.workspace_id, fixture.deployment_id, fixture.target_id]
    )

    hidden_submit =
      api_request(
        :post,
        "/api/orchestrator/v1/runs",
        %{
          "manifest_version_id" => fixture.version.manifest_version_id,
          "target" => %{"type" => "asset", "id" => fixture.target_id}
        },
        fixture: fixture,
        identity: identity,
        idempotency_key: "hidden-target"
      )

    assert hidden_submit.status in [400, 404, 422]

    hidden_inspection =
      api_request(
        :get,
        "/api/orchestrator/v1/manifests/#{fixture.version.manifest_version_id}/assets/#{fixture.target_id}/inspection",
        nil,
        fixture: fixture,
        identity: identity
      )

    assert hidden_inspection.status in [400, 404, 422]

    manifest =
      fixture.version.manifest
      |> Favn.Manifest.Serializer.encode_manifest!()
      |> Jason.decode!()

    publish =
      api_request(:post, "/api/orchestrator/v1/manifests", %{
        "manifest" => manifest,
        "manifest_version_id" => fixture.version.manifest_version_id,
        "content_hash" => fixture.version.content_hash,
        "schema_version" => fixture.version.schema_version,
        "runner_contract_version" => fixture.version.runner_contract_version,
        "runner_releases" => fixture.version.runner_releases,
        "serialization_format" => fixture.version.serialization_format
      })

    assert publish.status == 403
  end

  test "administrator bootstrap is one-time and recovery rotates credentials", fixture do
    share_repo_sandbox!()
    second_workspace_id = "ws-bootstrap-#{System.unique_integer([:positive])}"

    assert :ok =
             RegistryStore.provision_workspace(%ProvisionWorkspace{
               platform_context: fixture.platform_context,
               workspace_id: second_workspace_id,
               slug: second_workspace_id,
               display_name: "Second bootstrap workspace",
               occurred_at: DateTime.utc_now()
             })

    username = "bootstrap-#{System.unique_integer([:positive])}"
    password = "bootstrap-password-long"
    replacement_password = "replacement-password-long"

    parent = self()

    attempts =
      for attempt <- 1..2 do
        Task.async(fn ->
          send(parent, {:bootstrap_ready, attempt})

          receive do
            :bootstrap_start ->
              AdminLifecycle.bootstrap(
                [second_workspace_id, fixture.workspace_id],
                username,
                password,
                "Platform Administrator",
                identity_store: IdentityStore
              )
          end
        end)
      end

    assert_receive {:bootstrap_ready, 1}
    assert_receive {:bootstrap_ready, 2}
    Enum.each(attempts, &send(&1.pid, :bootstrap_start))

    results = Enum.map(attempts, &Task.await(&1, 15_000))

    assert [{:ok, bootstrapped}] = Enum.filter(results, &match?({:ok, _actor}, &1))
    assert [{:error, %Error{kind: :conflict}}] = Enum.reject(results, &match?({:ok, _actor}, &1))

    assert {:error, %Error{kind: :conflict}} =
             AdminLifecycle.bootstrap(
               [fixture.workspace_id],
               "second-admin",
               "second-bootstrap-password",
               "Second Administrator",
               identity_store: IdentityStore
             )

    {:ok, first_login_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:password", [:customer_reader])

    {:ok, second_login_context} =
      WorkspaceContext.new(second_workspace_id, "auth:password", [:customer_reader])

    assert {:ok, first_actor} =
             Identity.authenticate_password(first_login_context, username, password)

    assert {:ok, second_actor} =
             Identity.authenticate_password(second_login_context, username, password)

    assert first_actor.id == second_actor.id
    assert first_actor.id == bootstrapped.actor_id
    assert first_actor.roles == [:admin]
    assert first_actor.status == :active
    assert second_actor.roles == [:admin]
    assert second_actor.status == :active

    assert %{rows: [[["platform_admin"], "active"]]} =
             SQL.query!(
               Repo,
               """
               SELECT roles, status
               FROM favn_control.auth_platform_grants
               WHERE actor_id = $1
               """,
               [first_actor.id]
             )

    assert %{rows: membership_rows} =
             SQL.query!(
               Repo,
               """
               SELECT workspace_id, roles, status
               FROM favn_control.auth_workspace_memberships
               WHERE actor_id = $1
               ORDER BY workspace_id
               """,
               [first_actor.id]
             )

    assert membership_rows ==
             [
               [fixture.workspace_id, ["workspace_admin"], "active"],
               [second_workspace_id, ["workspace_admin"], "active"]
             ]
             |> Enum.sort()

    assert {:ok, session} =
             Identity.issue_session(first_login_context, first_actor.id,
               expected_credential_version: first_actor.credential_version
             )

    assert {:ok, second_session} =
             Identity.issue_session(second_login_context, second_actor.id,
               expected_credential_version: second_actor.credential_version
             )

    assert :ok =
             Identity.set_global_actor_status(
               fixture.platform_context,
               first_actor.id,
               :disabled,
               first_actor.version
             )

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(first_login_context, username, password)

    assert {:ok, recovered} =
             AdminLifecycle.recover(username, replacement_password, identity_store: IdentityStore)

    assert recovered.actor_id == first_actor.id

    assert {:error,
            %Error{
              kind: :conflict,
              details: %{reason_code: "credential_version_changed"}
            }} =
             Identity.issue_session(first_login_context, first_actor.id,
               expected_credential_version: first_actor.credential_version
             )

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(first_login_context, username, password)

    assert {:ok, recovered_actor} =
             Identity.authenticate_password(first_login_context, username, replacement_password)

    assert recovered_actor.id == first_actor.id

    assert {:error, :invalid_session} =
             Identity.introspect_session(first_login_context, session.token)

    assert {:error, :invalid_session} =
             Identity.introspect_session(second_login_context, second_session.token)

    {:ok, admin_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:admin-test", [:workspace_admin])

    viewer_username = "viewer-#{System.unique_integer([:positive])}"

    assert {:ok, viewer} =
             Identity.create_actor(
               admin_context,
               viewer_username,
               "viewer-password-long",
               "Viewer",
               [:viewer]
             )

    assert {:ok, authenticated_viewer} =
             Identity.authenticate_password(
               first_login_context,
               viewer_username,
               "viewer-password-long"
             )

    assert {:ok, viewer_session} =
             Identity.issue_session(first_login_context, viewer.id,
               expected_credential_version: authenticated_viewer.credential_version
             )

    assert {:error, %Error{kind: :forbidden}} =
             AdminLifecycle.recover(viewer_username, replacement_password,
               identity_store: IdentityStore
             )

    viewer_replacement_password = "viewer-replacement-password"

    assert {:ok, reset_viewer} =
             AdminLifecycle.reset_actor_credential(
               viewer_username,
               viewer_replacement_password,
               identity_store: IdentityStore
             )

    assert reset_viewer.actor_id == viewer.id

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(
               first_login_context,
               viewer_username,
               "viewer-password-long"
             )

    assert {:ok, reset_viewer_actor} =
             Identity.authenticate_password(
               first_login_context,
               viewer_username,
               viewer_replacement_password
             )

    assert reset_viewer_actor.id == viewer.id

    assert {:error, :invalid_session} =
             Identity.introspect_session(first_login_context, viewer_session.token)

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.auth_platform_audit_entries
               WHERE action = 'administrator.bootstrapped' AND subject_id = $1
               """,
               [first_actor.id]
             )

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.auth_platform_audit_entries
               WHERE action = 'actor.credential.reset' AND subject_id = $1
               """,
               [viewer.id]
             )

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.auth_platform_audit_entries
               WHERE action = 'administrator.credential.recovered' AND subject_id = $1
               """,
               [first_actor.id]
             )

    assert %{rows: audit_details} =
             SQL.query!(
               Repo,
               """
               SELECT detail
               FROM favn_control.auth_platform_audit_entries
               WHERE subject_id = $1
               UNION ALL
               SELECT detail
               FROM favn_control.auth_audit_entries
               WHERE subject_id = $1
               """,
               [first_actor.id]
             )

    audit_text = inspect(audit_details)
    refute audit_text =~ password
    refute audit_text =~ replacement_password
    refute audit_text =~ "password_hash"
    refute audit_text =~ "$argon2"
  end

  test "administrator recovery and actor credential reset replay exact commands", fixture do
    workspace_admin_username = "pre-bootstrap-admin-#{System.unique_integer([:positive])}"

    assert {:ok, _workspace_admin} =
             Identity.create_actor(
               fixture.workspace_context,
               workspace_admin_username,
               "pre-bootstrap-password",
               "Pre-bootstrap Workspace Administrator",
               [:admin]
             )

    username = "replay-admin-#{System.unique_integer([:positive])}"

    assert {:ok, administrator} =
             AdminLifecycle.bootstrap(
               [fixture.workspace_id],
               username,
               "initial-replay-password",
               "Replay Administrator",
               identity_store: IdentityStore
             )

    recovery = %RecoverAdministratorCredential{
      platform_context: fixture.platform_context,
      command_id: "admin-recovery-replay-#{System.unique_integer([:positive])}",
      username: username,
      password_hash: Credentials.hash_password("recovered-replay-password").password_hash,
      occurred_at: DateTime.utc_now()
    }

    administrator_id = administrator.actor_id

    assert {:ok, ^administrator_id} = IdentityStore.recover_administrator_credential(recovery)
    assert {:ok, ^administrator_id} = IdentityStore.recover_administrator_credential(recovery)

    changed_recovery = %{
      recovery
      | password_hash: Credentials.hash_password("changed-recovery-password").password_hash
    }

    assert {:error, %Error{kind: :conflict}} =
             IdentityStore.recover_administrator_credential(changed_recovery)

    {:ok, admin_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:replay-admin", [:workspace_admin])

    actor_username = "replay-actor-#{System.unique_integer([:positive])}"

    assert {:ok, actor} =
             Identity.create_actor(
               admin_context,
               actor_username,
               "initial-actor-password",
               "Replay Actor",
               [:viewer]
             )

    reset = %ResetActorCredential{
      platform_context: fixture.platform_context,
      command_id: "actor-reset-replay-#{System.unique_integer([:positive])}",
      username: actor_username,
      password_hash: Credentials.hash_password("reset-replay-password").password_hash,
      occurred_at: DateTime.utc_now()
    }

    actor_id = actor.id

    assert {:ok, ^actor_id} = IdentityStore.reset_actor_credential(reset)
    assert {:ok, ^actor_id} = IdentityStore.reset_actor_credential(reset)

    changed_reset = %{
      reset
      | password_hash: Credentials.hash_password("changed-reset-password").password_hash
    }

    assert {:error, %Error{kind: :conflict}} =
             IdentityStore.reset_actor_credential(changed_reset)

    assert %{rows: [[2]]} =
             SQL.query!(
               Repo,
               """
               SELECT count(*)
               FROM favn_control.auth_platform_audit_entries
               WHERE command_id IN ($1, $2)
               """,
               [recovery.command_id, reset.command_id]
             )
  end

  test "commits a projected batch after its lease deadline while retaining the cursor lock",
       fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    assert {:ok, publications} = Sequencer.sequence_batch()
    assert publications != []

    run_publication_id =
      publications
      |> Enum.find(&(&1.aggregate_id == run.id))
      |> Map.fetch!(:publication_id)

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.projection_cursors
        (projector_name, shard_id, last_publication_id, fencing_token, version, updated_at)
      VALUES ('control_plane_v1', 0, $1, 0, 1, clock_timestamp())
      ON CONFLICT (projector_name, shard_id) DO UPDATE
      SET last_publication_id = EXCLUDED.last_publication_id,
          owner_id = NULL,
          claim_expires_at = NULL,
          updated_at = EXCLUDED.updated_at
      """,
      [run_publication_id - 1]
    )

    SQL.query!(
      Repo,
      """
      CREATE FUNCTION favn_control.test_delay_target_status_projection()
      RETURNS trigger AS $$
      BEGIN
        PERFORM pg_sleep(0.025);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
      """,
      []
    )

    SQL.query!(
      Repo,
      """
      CREATE TRIGGER test_delay_target_status_projection
      BEFORE INSERT OR UPDATE ON favn_control.target_statuses
      FOR EACH ROW
      EXECUTE FUNCTION favn_control.test_delay_target_status_projection()
      """,
      []
    )

    assert {:ok, %{count: count, last_publication_id: last_publication_id}} =
             Projector.project_batch("short-lease:" <> run.id,
               limit: 250,
               lease_duration_ms: 1
             )

    assert count >= 1
    assert last_publication_id >= run_publication_id
  end

  test "projects ordered compact operator read models without group scans", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    running = RunState.transition(run, status: :running)

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "project-running:" <> run.id,
               expected_sequence: 1,
               run: running,
               event: %{
                 run_id: run.id,
                 sequence: 2,
                 event_type: :run_started,
                 status: :running,
                 occurred_at: DateTime.utc_now()
               }
             })

    assert {:ok, publications} = Sequencer.sequence_batch()
    assert publications != []

    assert drain_projector("node-a") >= length(publications)

    assert {:ok, group_page} =
             OperatorReadStore.page_execution_groups(%PageExecutionGroups{
               scope: fixture.workspace_context,
               limit: 10
             })

    assert [group] = group_page.items
    assert group.root_run_id == run.id
    assert group.status == :running
    assert group.run_count == 1
    assert group.running_count == 1

    # A page of groups has to say what each run was for and what triggered it,
    # or a runs list cannot be read. These come from the root run, not from the
    # group's own row, so they are the part most easily lost.
    assert group.target_refs != []
    assert group.trigger_type != nil
    assert %DateTime{} = group.started_at
    assert is_nil(group.finished_at), "a running group has not finished"

    assert {:ok, detail} =
             OperatorReadStore.get_execution_group(%GetExecutionGroup{
               workspace_context: fixture.workspace_context,
               root_run_id: run.id,
               detail_limit: 10
             })

    assert [summary] = detail.runs.items
    assert summary.run_id == run.id
    assert summary.status == :running

    assert {:ok, [target_status]} =
             OperatorReadStore.get_target_statuses(%GetTargetStatuses{
               workspace_context: fixture.workspace_context,
               manifest_version_id: fixture.version.manifest_version_id,
               target_kind: :asset,
               target_ids: [fixture.target_id]
             })

    assert target_status.status == :running
    assert target_status.run_id == run.id

    %{rows: [[status_publication_id]]} =
      SQL.query!(
        Repo,
        """
        SELECT source_publication_id
        FROM favn_control.target_statuses
        WHERE workspace_id = $1 AND deployment_id = $2
          AND target_kind = 'asset' AND target_id = $3
        """,
        [fixture.workspace_id, fixture.deployment_id, fixture.target_id]
      )

    progressed =
      RunState.transition(running,
        metadata: Map.put(running.metadata, :projected_step, true)
      )

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "project-step:" <> run.id,
               expected_sequence: 2,
               run: progressed,
               event: %{
                 run_id: run.id,
                 sequence: 3,
                 event_type: :step_running,
                 status: :running,
                 data: %{
                   asset_step_id: "step:" <> run.id,
                   asset_ref: {MyApp.Asset, :asset},
                   attempt: 1,
                   window: %{
                     key: "requested:2026-07-20",
                     kind: :day,
                     start_at: ~U[2026-07-13 00:00:00Z],
                     end_at: ~U[2026-07-21 00:00:00Z],
                     timezone: "Etc/UTC"
                   }
                 },
                 occurred_at: DateTime.utc_now()
               }
             })

    assert {:ok, [_publication]} = Sequencer.sequence_batch()
    assert drain_projector("node-a") >= 1

    assert {:ok, compact} =
             OperatorReadStore.get_operator_run_overview(%GetOperatorRunOverview{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 10
             })

    assert [attempt] = compact.attempts
    assert attempt.asset_step_id == "step:" <> run.id
    assert attempt.window.start_at == ~U[2026-07-13 00:00:00Z]
    assert String.starts_with?(attempt.window_identity, "runtime:")
    assert compact.attempt_counts.total == 1
    assert compact.attempt_counts.running == 1
    assert compact.attempt_counts.effective_windows == 1
    refute compact.attempts_truncated?
    refute compact.runs_truncated?
    refute compact.requested_windows_truncated?

    assert compact.root_run.runner_releases == fixture.version.runner_releases

    blocked_progress =
      RunState.transition(progressed,
        metadata: Map.put(progressed.metadata, :projected_blocked_step, true)
      )

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "project-step-blocked:" <> run.id,
               expected_sequence: 3,
               run: blocked_progress,
               event: %{
                 run_id: run.id,
                 sequence: 4,
                 event_type: :step_blocked,
                 status: :running,
                 data: %{
                   asset_step_id: "step:" <> run.id,
                   asset_ref: {MyApp.Asset, :asset},
                   node_result: %{error: :upstream_blocked}
                 },
                 occurred_at: DateTime.utc_now()
               }
             })

    assert {:ok, [_publication]} = Sequencer.sequence_batch()
    assert drain_projector("node-a") >= 1

    assert {:ok, blocked_compact} =
             OperatorReadStore.get_operator_run_overview(%GetOperatorRunOverview{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 10
             })

    assert [%{status: :blocked, error: "upstream_blocked"}] = blocked_compact.attempts
    assert blocked_compact.attempt_counts.failed == 1

    assert Enum.all?(compact.runs, fn summary ->
             summary.runner_releases == fixture.version.runner_releases
           end)

    %{rows: [[^status_publication_id]]} =
      SQL.query!(
        Repo,
        """
        SELECT source_publication_id
        FROM favn_control.target_statuses
        WHERE workspace_id = $1 AND deployment_id = $2
          AND target_kind = 'asset' AND target_id = $3
        """,
        [fixture.workspace_id, fixture.deployment_id, fixture.target_id]
      )

    completed = RunState.transition(blocked_progress, status: :ok)

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "project-step-finished:" <> run.id,
               expected_sequence: 4,
               run: completed,
               event: %{
                 run_id: run.id,
                 sequence: 5,
                 event_type: :step_finished,
                 status: :ok,
                 data: %{
                   asset_step_id: "step:" <> run.id,
                   asset_ref: {MyApp.Asset, :asset},
                   attempt: 1,
                   window: %{
                     key: "requested:2026-07-20",
                     kind: :day,
                     start_at: ~U[2026-07-13 00:00:00Z],
                     end_at: ~U[2026-07-21 00:00:00Z],
                     timezone: "Etc/UTC"
                   }
                 },
                 occurred_at: DateTime.utc_now()
               }
             })

    assert {:ok, [_publication]} = Sequencer.sequence_batch()
    assert drain_projector("node-a") >= 1

    assert {:ok, completed_compact} =
             OperatorReadStore.get_operator_run_overview(%GetOperatorRunOverview{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               limit: 10
             })

    assert [%{status: :ok}] = completed_compact.attempts
    assert completed_compact.attempt_counts.total == 1
    assert completed_compact.attempt_counts.completed == 1
    assert completed_compact.attempt_counts.running == 0

    assert {:ok, target_runs} =
             OperatorReadStore.page_target_runs(%PageTargetRuns{
               workspace_context: fixture.workspace_context,
               deployment_id: fixture.deployment_id,
               target_kind: :asset,
               target_id: fixture.target_id,
               limit: 10
             })

    assert [target_run] = target_runs.items
    assert target_run.run_id == run.id
    assert target_run.runner_releases == fixture.version.runner_releases

    {:ok, platform_context} =
      PlatformContext.new("consultant", "read-grant:" <> run.id, [:platform_reader])

    assert {:ok, manifests} =
             OperatorReadStore.page_manifests(%PageManifests{
               platform_context: platform_context,
               limit: 10
             })

    assert Enum.any?(
             manifests.items,
             &(&1.manifest_version_id == fixture.version.manifest_version_id)
           )
  end

  test "a page of execution groups narrows in the store rather than in the caller", fixture do
    {first_command, first_run} = create_run_command(fixture)
    {second_command, second_run} = create_run_command(fixture)

    assert {:ok, _created} = RunStore.create_run(first_command)
    assert {:ok, _created} = RunStore.create_run(second_command)

    assert {:ok, publications} = Sequencer.sequence_batch()
    assert publications != []

    assert drain_projector("node-a") >= length(publications)

    ids = fn filters ->
      query = struct!(%PageExecutionGroups{scope: fixture.workspace_context, limit: 10}, filters)
      assert {:ok, page} = OperatorReadStore.page_execution_groups(query)
      Enum.map(page.items, & &1.root_run_id)
    end

    both = ids.([])
    assert Enum.sort(both) == Enum.sort([first_run.id, second_run.id])

    # Ordering is by when the root run started, so reversing the order reverses
    # the page rather than producing some third arrangement.
    assert ids.(order: :started_asc) == Enum.reverse(both)

    assert ids.(search: first_run.id) == [first_run.id]
    assert Enum.sort(ids.(search: "myapp.ass")) == Enum.sort(both)
    assert ids.(search: "no-such-target") == []
    assert ids.(search: "   ") == both

    # A wildcard the operator typed is a character to look for, not a pattern.
    assert ids.(search: "%") == []

    assert {:ok, %{items: [reference | _]}} =
             OperatorReadStore.page_execution_groups(%PageExecutionGroups{
               scope: fixture.workspace_context,
               limit: 1
             })

    assert Enum.sort(ids.(trigger_type: reference.trigger_type)) == Enum.sort(both)
    assert ids.(trigger_type: :resource_recovery) == []

    now = DateTime.utc_now()
    assert ids.(started_after: DateTime.add(now, 3600, :second)) == []
    assert ids.(started_before: DateTime.add(now, -3600, :second)) == []
    assert Enum.sort(ids.(started_after: DateTime.add(now, -3600, :second))) == Enum.sort(both)

    assert {:ok, first_page} =
             OperatorReadStore.page_execution_groups(%PageExecutionGroups{
               scope: fixture.workspace_context,
               limit: 1
             })

    assert first_page.has_more?
    assert [head] = Enum.map(first_page.items, & &1.root_run_id)

    assert {:ok, next_page} =
             OperatorReadStore.page_execution_groups(%PageExecutionGroups{
               scope: fixture.workspace_context,
               limit: 1,
               after: first_page.next_cursor
             })

    assert [tail] = Enum.map(next_page.items, & &1.root_run_id)
    assert [head, tail] == both

    counts = fn filters ->
      query = struct!(%CountExecutionGroups{scope: fixture.workspace_context}, filters)
      assert {:ok, counts} = OperatorReadStore.count_execution_groups(query)
      counts
    end

    unfiltered = counts.([])
    assert unfiltered.total == 2
    assert unfiltered.active == 2
    assert unfiltered.failed == 0
    assert unfiltered.succeeded == 0

    # A count offered next to a filter has to be the size of what that filter
    # returns, so every narrowing the page accepts narrows the counts too.
    assert counts.(search: first_run.id).total == 1
    assert counts.(search: "no-such-target").total == 0
    assert counts.(trigger_type: :resource_recovery).total == 0
    assert counts.(started_after: DateTime.add(now, 3600, :second)).active == 0
    assert counts.(started_before: DateTime.add(now, -3600, :second)).total == 0

    for invalid <- [[started_after: "today"], [order: :duration_desc], [trigger_type: :nonsense]] do
      query = struct!(%PageExecutionGroups{scope: fixture.workspace_context, limit: 10}, invalid)
      assert {:error, %Error{kind: :invalid}} = OperatorReadStore.page_execution_groups(query)
    end

    for invalid <- [[started_after: "today"], [trigger_type: :nonsense], [search: 7]] do
      query = struct!(%CountExecutionGroups{scope: fixture.workspace_context}, invalid)
      assert {:error, %Error{kind: :invalid}} = OperatorReadStore.count_execution_groups(query)
    end
  end

  # A backfill's root run is terminal the instant it is created — it exists to group
  # the window runs that do the work. Recording the root run's `terminal_at` as the
  # group's finish therefore reported every backfill as finishing before it started,
  # with a duration of zero. The group finishes when nothing in it is outstanding.
  test "a group finishes when its last run settles, not when its root run does",
       fixture do
    {root_command, root} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(root_command)

    child_id = "run-child-#{System.unique_integer([:positive])}"
    {child_command, child} = create_run_command(fixture, child_id)
    child = %{child | root_run_id: root.id}

    assert {:ok, _created} = RunStore.create_run(%{child_command | run: child})

    settle = fn run, sequence, command ->
      finished =
        RunState.transition(
          run,
          [status: :ok, metadata: Map.put(run.metadata, :terminal_event_type, :run_finished)],
          DateTime.utc_now()
        )

      assert {:ok, _committed} =
               RunStore.commit_transition(%CommitRunTransition{
                 workspace_context: fixture.workspace_context,
                 command_id: command,
                 expected_sequence: sequence,
                 run: finished,
                 event: %{
                   run_id: run.id,
                   sequence: sequence + 1,
                   event_type: :run_finished,
                   status: :ok,
                   occurred_at: DateTime.utc_now()
                 }
               })

      finished
    end

    settle.(root, 1, "settle-root:" <> root.id)

    overview = fn ->
      assert {:ok, publications} = Sequencer.sequence_batch()

      assert drain_projector("node-a") >= length(publications)

      Repo.get_by!(FavnStoragePostgres.Schemas.ExecutionGroupOverview,
        workspace_id: fixture.workspace_id,
        root_run_id: root.id
      )
    end

    # The root has settled but the child has not, so the group has not finished.
    outstanding = overview.()
    assert outstanding.run_count == 2
    assert is_nil(outstanding.finished_at)

    settle.(child, 1, "settle-child:" <> child.id)

    settled = overview.()
    assert settled.status == "succeeded"
    assert %DateTime{} = settled.finished_at
    assert DateTime.compare(settled.finished_at, settled.started_at) == :gt

    # The root run stopped first, and taking its instant is what this rules out.
    assert %{rows: [[%DateTime{} = root_terminal_at]]} =
             SQL.query!(
               Repo,
               "SELECT terminal_at FROM favn_control.runs WHERE workspace_id = $1 AND run_id = $2",
               [fixture.workspace_id, root.id]
             )

    assert DateTime.compare(settled.finished_at, root_terminal_at) == :gt
  end

  # A group's own counters count runs, which is one for everything but a backfill.
  # What the list reports is asset steps, so they are counted per group in one
  # bounded aggregate rather than inferred from the run count.
  test "an execution group page counts the asset steps its runs recorded", fixture do
    {command, run} = create_run_command(fixture)
    {other_command, other_run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)
    assert {:ok, _created} = RunStore.create_run(other_command)
    assert {:ok, publications} = Sequencer.sequence_batch()
    assert publications != []

    assert drain_projector("node-a") >= length(publications)

    for {step, status} <- [{"step-1", "ok"}, {"step-2", "error"}, {"step-3", "queued"}] do
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.asset_attempt_overviews
          (workspace_id, root_run_id, run_id, asset_step_id, asset_ref, window_identity,
           status, source_publication_id, updated_at)
        VALUES ($1, $2, $3, $4, $5, 'none', $6, 1, now())
        """,
        [fixture.workspace_context.workspace_id, run.id, run.id, step, "MyApp.Assets.a", status]
      )
    end

    assert {:ok, page} =
             OperatorReadStore.page_execution_groups(%PageExecutionGroups{
               scope: fixture.workspace_context,
               limit: 10
             })

    assert %{asset_counts: counts} = Enum.find(page.items, &(&1.root_run_id == run.id))
    assert counts == %{total: 3, completed: 2, failed: 1, running: 0, queued: 1}

    # A group whose steps have not been recorded reports nothing rather than
    # borrowing its run count.
    assert %{asset_counts: %{total: 0, completed: 0}} =
             Enum.find(page.items, &(&1.root_run_id == other_run.id))
  end

  # The runs list reads this every page and again on every live refresh, so its
  # order has to come from an index rather than from sorting the workspace. The
  # sort key is projected onto the group for exactly this reason: ordering it on
  # the root run's insert time meant a join and a sort with no index to serve it.
  test "paging execution groups by start time is index-ordered in both directions",
       fixture do
    plans =
      Repo.transaction(fn ->
        SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])
        SQL.query!(Repo, "SET LOCAL enable_sort = off", [])

        for order <- ["DESC NULLS LAST", "ASC NULLS FIRST"] do
          direction = if order == "DESC NULLS LAST", do: "DESC", else: "ASC"

          %{rows: rows} =
            SQL.query!(
              Repo,
              """
              EXPLAIN (FORMAT TEXT)
              SELECT root_run_id
              FROM favn_control.execution_group_overviews
              WHERE workspace_id = $1 AND started_at >= $2
              ORDER BY started_at #{order},
                       workspace_id #{direction},
                       root_run_id #{direction}
              LIMIT 51
              """,
              [
                fixture.workspace_context.workspace_id,
                DateTime.add(DateTime.utc_now(), -86_400, :second)
              ]
            )

          rows |> List.flatten() |> Enum.join("\n")
        end
      end)
      |> then(fn {:ok, plans} -> plans end)

    for plan <- plans do
      assert plan =~ "started_idx"
      refute plan =~ "Sort"
    end

    assert Enum.at(plans, 1) =~ "Backward"
  end

  test "outbox publication preserves run causality when business clocks move backwards",
       fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    future = DateTime.add(DateTime.utc_now(), 3_600, :second)
    past = DateTime.add(future, -7_200, :second)
    running = RunState.transition(run, [status: :running], future)

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "clock-skew-running:" <> run.id,
               expected_sequence: 1,
               run: running,
               event: %{
                 run_id: run.id,
                 sequence: 2,
                 event_type: :run_started,
                 status: :running,
                 occurred_at: future
               }
             })

    finished =
      RunState.transition(
        running,
        [status: :ok, metadata: Map.put(running.metadata, :terminal_event_type, :run_finished)],
        past
      )

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "clock-skew-finished:" <> run.id,
               expected_sequence: 2,
               run: finished,
               event: %{
                 run_id: run.id,
                 sequence: 3,
                 event_type: :run_finished,
                 status: :ok,
                 occurred_at: past
               }
             })

    assert {:ok, publications} = Sequencer.sequence_batch()
    assert publications != []

    assert %{rows: [[1, first], [2, second], [3, third]]} =
             SQL.query!(
               Repo,
               """
               SELECT aggregate_version, publication_id
               FROM favn_control.outbox_events
               WHERE workspace_id = $1 AND aggregate_kind = 'run' AND aggregate_id = $2
               ORDER BY publication_id
               """,
               [fixture.workspace_id, run.id]
             )

    assert first < second and second < third

    assert drain_projector("clock-skew-projector") >= length(publications)

    assert {:ok, [target_status]} =
             OperatorReadStore.get_target_statuses(%GetTargetStatuses{
               workspace_context: fixture.workspace_context,
               manifest_version_id: fixture.version.manifest_version_id,
               target_kind: :asset,
               target_ids: [fixture.target_id]
             })

    assert target_status.status == :ok
    assert target_status.run_id == run.id
  end

  test "builds, verifies, claims, and transitions a resumable backfill plan", fixture do
    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    now = DateTime.utc_now()

    windows = [
      %BackfillPlanWindow{
        window_id: "window-a:" <> run.id,
        window_key: "2026-07-15",
        window_start: DateTime.add(now, -172_800, :second),
        window_end: DateTime.add(now, -86_400, :second),
        payload: %{"partition" => "2026-07-15"}
      },
      %BackfillPlanWindow{
        window_id: "window-b:" <> run.id,
        window_key: "2026-07-16",
        window_start: DateTime.add(now, -86_400, :second),
        window_end: now,
        payload: %{"partition" => "2026-07-16"}
      }
    ]

    batch_hash = BackfillPlan.batch_hash(windows)
    backfill_id = "backfill:" <> run.id

    start = %StartBackfillPlan{
      workspace_context: fixture.workspace_context,
      command_id: "backfill-start:" <> run.id,
      backfill_id: backfill_id,
      root_run_id: run.id,
      deployment_id: fixture.deployment_id,
      manifest_version_id: fixture.version.manifest_version_id,
      target_kind: :asset,
      target_id: fixture.target_id,
      range_start: hd(windows).window_start,
      range_end: List.last(windows).window_end,
      expected_window_count: 2,
      expected_batch_count: 1,
      plan_hash: BackfillPlan.plan_hash([batch_hash]),
      occurred_at: now
    }

    assert {:ok, planning} = BackfillStore.start_plan(start)
    assert planning.status == :planning
    assert {:ok, ^planning} = BackfillStore.start_plan(start)

    append = %AppendBackfillPlanBatch{
      workspace_context: fixture.workspace_context,
      command_id: "backfill-batch:" <> run.id,
      backfill_id: backfill_id,
      batch_index: 0,
      batch_hash: batch_hash,
      windows: windows,
      occurred_at: now
    }

    assert {:ok, appended} = BackfillStore.append_plan_batch(append)
    assert appended.appended_window_count == 2
    assert {:ok, ^appended} = BackfillStore.append_plan_batch(append)

    activate = %ActivateBackfillPlan{
      workspace_context: fixture.workspace_context,
      command_id: "backfill-activate:" <> run.id,
      backfill_id: backfill_id,
      expected_version: appended.version,
      occurred_at: now
    }

    assert {:ok, ready} = BackfillStore.activate_plan(activate)
    assert ready.status == :ready
    assert {:ok, ^ready} = BackfillStore.activate_plan(activate)

    claim = %ClaimBackfillWindows{
      workspace_context: fixture.workspace_context,
      batch_id: "backfill-claim:" <> run.id,
      owner_id: "backfill-worker-a",
      lease_duration_ms: 30_000,
      backfill_id: backfill_id,
      limit: 1
    }

    assert {:ok, [claimed]} = BackfillStore.claim_windows(claim)
    assert claimed.status == :claimed
    assert claimed.fencing_token == 1
    assert {:ok, [^claimed]} = BackfillStore.claim_windows(claim)

    running_command = %TransitionBackfillWindow{
      workspace_context: fixture.workspace_context,
      command_id: "backfill-running:" <> run.id,
      backfill_id: backfill_id,
      window_id: claimed.window_id,
      owner_id: "backfill-worker-a",
      fencing_token: claimed.fencing_token,
      expected_version: claimed.version,
      status: :running,
      run_id: run.id,
      occurred_at: now
    }

    assert {:ok, running} = BackfillStore.transition_window(running_command)
    assert running.status == :running

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.backfill_windows
      SET claim_expires_at = clock_timestamp() - interval '1 second'
      WHERE workspace_id = $1 AND backfill_id = $2 AND window_id = $3
      """,
      [fixture.workspace_id, backfill_id, running.window_id]
    )

    reclaim = %{
      claim
      | batch_id: "backfill-reclaim:" <> run.id,
        owner_id: "backfill-worker-b"
    }

    assert {:ok, [reclaimed]} = BackfillStore.claim_windows(reclaim)
    assert reclaimed.status == :claimed
    assert is_nil(reclaimed.run_id)
    assert reclaimed.attempt_count == running.attempt_count + 1
    assert reclaimed.fencing_token == running.fencing_token + 1

    assert {:ok, resumed} =
             BackfillStore.transition_window(%{
               running_command
               | command_id: "backfill-resumed:" <> run.id,
                 owner_id: reclaimed.claim_owner,
                 fencing_token: reclaimed.fencing_token,
                 expected_version: reclaimed.version,
                 status: :running
             })

    assert {:ok, succeeded} =
             BackfillStore.transition_window(%{
               running_command
               | command_id: "backfill-succeeded:" <> run.id,
                 owner_id: resumed.claim_owner,
                 fencing_token: resumed.fencing_token,
                 expected_version: resumed.version,
                 status: :succeeded
             })

    assert succeeded.status == :succeeded

    assert {:ok, fetched} =
             BackfillStore.get_backfill(%GetBackfill{
               workspace_context: fixture.workspace_context,
               backfill_id: backfill_id
             })

    assert fetched.backfill_id == backfill_id

    assert {:ok, page} =
             BackfillStore.page_windows(%PageBackfillWindows{
               workspace_context: fixture.workspace_context,
               backfill_id: backfill_id,
               limit: 1
             })

    assert length(page.items) == 1
    assert page.has_more?
  end

  defp provision_deploy_fixture(version \\ nil, extra_targets \\ []) do
    unique = Integer.to_string(System.unique_integer([:positive]))
    workspace_id = "ws-#{unique}"
    deployment_id = "deploy-#{unique}"
    now = DateTime.utc_now()

    {:ok, platform_context} =
      PlatformContext.new("consultant", "grant-#{unique}", [:platform_admin])

    :ok =
      RegistryStore.provision_workspace(%ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "customer-#{unique}",
        display_name: "Customer #{unique}",
        occurred_at: now
      })

    {version, packages} =
      case version do
        nil -> manifest_publication("mv_#{unique}")
        %Version{} = supplied -> {supplied, []}
      end

    if packages != [] do
      assert :ok =
               RegistryStore.register_execution_packages(%RegisterExecutionPackages{
                 platform_context: platform_context,
                 packages: packages
               })
    end

    unless match?(
             {:ok, _version},
             RegistryStore.get_manifest(
               %FavnOrchestrator.Persistence.Queries.ManifestSelector.ById{
                 manifest_version_id: version.manifest_version_id
               }
             )
           ) do
      assert {:ok, ^version} =
               RegistryStore.register_manifest(%RegisterManifest{
                 platform_context: platform_context,
                 version: version
               })
    end

    {:ok, workspace_context} =
      WorkspaceContext.new(workspace_id, "consultant", [:workspace_admin])

    target_id = TargetStatus.target_id_for_asset({MyApp.Asset, :asset})
    pipeline_target_id = TargetStatus.target_id_for_pipeline({MyApp.Pipeline, :daily})

    deploy_command = %DeployManifest{
      platform_context: platform_context,
      workspace_context: workspace_context,
      deployment_id: deployment_id,
      manifest_version_id: version.manifest_version_id,
      configuration: %{
        "secret_store_url" => "https://#{workspace_id}.vault.example.test",
        "resources" => %{"ducklake" => %{"secret_ref" => "ducklake-metadata"}}
      },
      targets:
        [
          %DeploymentTarget{
            target_kind: :asset,
            target_id: target_id,
            selection_source: :common,
            customer_visible: true,
            descriptor: %{"target_id" => target_id, "label" => target_id}
          },
          %DeploymentTarget{
            target_kind: :pipeline,
            target_id: pipeline_target_id,
            selection_source: :common,
            customer_visible: true,
            descriptor: %{
              "target_id" => pipeline_target_id,
              "label" => pipeline_target_id
            }
          }
        ] ++ extra_targets,
      schedules: [
        %DeploymentSchedule{
          pipeline_target_id: pipeline_target_id,
          schedule_id: "daily",
          schedule_fingerprint: "schedule-fingerprint-daily",
          definition: %{
            "pipeline_module" => "Elixir.MyApp.Pipeline",
            "pipeline_name" => "daily",
            "cron" => "0 0 * * *",
            "timezone" => "Etc/UTC",
            "overlap" => "forbid",
            "missed" => "skip",
            "window" => %{"kind" => "day", "timezone" => "Etc/UTC"}
          },
          next_due_at: DateTime.add(now, -1, :second),
          cursor: %{}
        }
      ],
      capacity_scopes: [
        %DeploymentCapacityScope{
          scope_id: "workspace:" <> workspace_id,
          scope_kind: :workspace,
          scope_key: workspace_id,
          capacity_limit: 1
        }
      ],
      occurred_at: now
    }

    assert {:ok, _runtime} = RegistryStore.deploy_manifest(deploy_command)

    %{
      workspace_id: workspace_id,
      platform_context: platform_context,
      deployment_id: deployment_id,
      workspace_context: workspace_context,
      version: version,
      target_id: target_id,
      pipeline_target_id: pipeline_target_id,
      capacity_scope_id: "workspace:" <> workspace_id,
      deploy_command: deploy_command
    }
  end

  defp admit_command(fixture, run_id, suffix) do
    %AdmitExecution{
      workspace_context: fixture.workspace_context,
      command_id: "admit:#{run_id}:#{suffix}",
      lease_id: "lease:#{run_id}:#{suffix}",
      waiter_id: "waiter:#{run_id}:#{suffix}",
      run_id: run_id,
      step_id: "step:#{suffix}",
      owner_id: "worker-a",
      owner_generation: 1,
      lease_duration_ms: 30_000,
      waiter_ttl_ms: 60_000,
      requests: [%CapacityRequest{scope_id: fixture.capacity_scope_id}],
      occurred_at: DateTime.utc_now()
    }
  end

  defp evidence_generation_id(fixture) do
    assert {:ok, [binding]} =
             TargetGenerationStore.get_evidence_bindings(%GetEvidenceBindings{
               workspace_context: fixture.workspace_context,
               target_ids: [fixture.target_id]
             })

    binding.evidence_generation_id
  end

  defp create_run_command(fixture, run_id \\ nil) do
    run_id = run_id || "run-#{System.unique_integer([:positive])}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        runner_releases: fixture.version.runner_releases,
        asset_ref: {MyApp.Asset, :asset},
        target_refs: [{MyApp.Asset, :asset}]
      )

    command = %CreateRun{
      workspace_context: fixture.workspace_context,
      command_id: "create:" <> run_id,
      deployment_id: fixture.deployment_id,
      run: run,
      targets: [
        %RunTarget{
          target_kind: :asset,
          target_id: fixture.target_id,
          target_module: "MyApp.Asset",
          target_name: "asset",
          is_primary: true
        }
      ],
      event: %{
        run_id: run_id,
        sequence: 1,
        event_type: :run_submitted,
        status: :pending,
        occurred_at: run.inserted_at
      }
    }

    {command, run}
  end

  defp schedule_submission(fixture, suffix) do
    run_id = "run-schedule-#{suffix}-#{fixture.workspace_id}"

    %EnqueueRunSubmission{
      workspace_context: fixture.workspace_context,
      command_id: "enqueue-schedule-#{suffix}-#{fixture.workspace_id}",
      submission_id: "submission-schedule-#{suffix}-#{fixture.workspace_id}",
      source: :scheduler,
      idempotency_key: "schedule-#{suffix}-#{fixture.workspace_id}",
      request_hash: :crypto.hash(:sha256, "schedule-#{suffix}-#{fixture.workspace_id}"),
      deployment_id: fixture.deployment_id,
      manifest_version_id: fixture.version.manifest_version_id,
      target_kind: "pipeline",
      target_id: fixture.pipeline_target_id,
      run_id: run_id,
      intent: %{"format" => "test", "payload" => "schedule"},
      occurred_at: DateTime.utc_now()
    }
  end

  defp pipeline_run_command(fixture) do
    run_id = "pipeline-run-#{System.unique_integer([:positive])}"
    ref = {MyApp.Asset, :asset}
    node_key = {ref, nil}

    plan = %Favn.Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      dependencies: :all,
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
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
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        runner_releases: fixture.version.runner_releases,
        asset_ref: ref,
        target_refs: [ref],
        submit_kind: :pipeline,
        plan: plan
      )

    command = %CreateRun{
      workspace_context: fixture.workspace_context,
      command_id: "create:" <> run_id,
      deployment_id: fixture.deployment_id,
      run: run,
      targets: [
        %RunTarget{
          target_kind: :asset,
          target_id: fixture.target_id,
          target_module: "MyApp.Asset",
          target_name: "asset",
          is_primary: true
        }
      ],
      event: %{
        run_id: run_id,
        sequence: 1,
        event_type: :run_submitted,
        status: :pending,
        occurred_at: run.inserted_at
      }
    }

    {command, run}
  end

  defp manifest_publication(manifest_version_id) do
    ref = {MyApp.Asset, :asset}
    package = runtime_input_execution_package(ref)
    private_ref = {MyApp.PrivateAsset, :private}
    private_package = runtime_input_execution_package(private_ref)

    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: ref,
          module: MyApp.Asset,
          name: :asset,
          type: :sql,
          freshness: Policy.from_value!(max_age: {:days, 1}),
          execution_package_hash: package.content_hash
        },
        %Favn.Manifest.Asset{
          ref: private_ref,
          module: MyApp.PrivateAsset,
          name: :private,
          type: :sql,
          execution_package_hash: private_package.content_hash
        }
      ],
      pipelines: [
        %Favn.Manifest.Pipeline{
          module: MyApp.Pipeline,
          name: :daily,
          selectors: [{:asset, {MyApp.Asset, :asset}}],
          window: Favn.Window.Policy.new!(:day, timezone: "Etc/UTC")
        }
      ]
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_contract()
        |> FavnTestSupport.with_manifest_graph(),
        manifest_version_id: manifest_version_id
      )

    {version, [package, private_package]}
  end

  defp target_descriptor(fixture, manifest_schema_version \\ nil) do
    asset = Enum.find(fixture.version.manifest.assets, &(&1.ref == {MyApp.Asset, :asset}))
    manifest_schema_version = manifest_schema_version || fixture.version.schema_version

    asset
    |> Map.from_struct()
    |> Map.merge(%{
      relation:
        RelationRef.new!(
          connection: :warehouse,
          schema: "analytics",
          name: "asset"
        ),
      materialization: :table
    })
    |> TargetDescriptor.from_asset(
      connection_definitions: %{
        warehouse: %{adapter: FavnTestSupport.TargetAdapter, module: nil}
      },
      manifest_schema_version: manifest_schema_version,
      runner_contract_version: fixture.version.runner_contract_version
    )
  end

  # A projector claim is deliberately held until its lease expires, and suites
  # that share this database commit real claims. Release any stale claim first so
  # draining under a fresh owner id is never rejected as `projection stream is
  # owned`, which previously surfaced as a CaseClauseError here.
  defp drain_projector(owner_id) do
    release_projection_cursor!()
    drain_projector(owner_id, 0)
  end

  defp drain_projector(owner_id, total) do
    case Projector.project_batch(owner_id, limit: 250) do
      {:ok, %{count: 250}} -> drain_projector(owner_id, total + 250)
      {:ok, %{count: count}} -> total + count
    end
  end

  defp release_projection_cursor! do
    SQL.query!(
      Repo,
      """
      UPDATE favn_control.projection_cursors
      SET owner_id = NULL, claim_expires_at = NULL
      WHERE projector_name = 'control_plane_v1' AND shard_id = 0
      """,
      []
    )

    :ok
  end

  defp api_identity(fixture, roles) do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "http-actor-#{suffix}"
    password = "http-boundary-password-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               password,
               "HTTP Boundary Actor",
               roles
             )

    assert {:ok, login_context} =
             WorkspaceContext.new(fixture.workspace_id, "auth:http-test", [:customer_reader])

    assert {:ok, authenticated} =
             Identity.authenticate_password(login_context, username, password)

    assert {:ok, session} =
             Identity.issue_session(login_context, actor.id,
               expected_credential_version: authenticated.credential_version
             )

    %{actor: actor, session: session, credential_version: authenticated.credential_version}
  end

  defp execution_package(ref) do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/storage_v2/core_authority_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
    package
  end

  defp runtime_input_execution_package(ref) do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/storage_v2/runtime_input_package.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} =
      ExecutionPackage.new(ref, %SQLExecution{
        sql: sql,
        template: template,
        runtime_inputs: %Favn.RuntimeInputResolver.Ref{module: MyApp.RuntimeInputResolver}
      })

    package
  end

  defp packaged_manifest_version(ref, package_hash) do
    asset = %Favn.Manifest.Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      execution_package_hash: package_hash
    }

    {:ok, version} =
      Version.new(
        FavnTestSupport.with_manifest_contract(%Manifest{
          assets: [asset],
          graph: %Graph{nodes: [ref], topo_order: [ref]}
        }),
        manifest_version_id: "mv-packaged-#{System.unique_integer([:positive])}"
      )

    version
  end

  defp packaged_manifest_version(count) when is_integer(count) and count > 0 do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/storage_v2/package_validation_batch.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    packages =
      Enum.map(1..count, fn index ->
        ref = {MyApp.PackageValidationAsset, String.to_atom("asset_#{index}")}
        {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
        package
      end)

    assets =
      Enum.map(packages, fn package ->
        {module, name} = package.asset_ref

        %Favn.Manifest.Asset{
          ref: package.asset_ref,
          module: module,
          name: name,
          type: :sql,
          execution_package_hash: package.content_hash
        }
      end)

    refs = Enum.map(assets, & &1.ref)

    {:ok, version} =
      Version.new(
        FavnTestSupport.with_manifest_contract(%Manifest{
          assets: assets,
          graph: %Graph{nodes: refs, topo_order: refs}
        }),
        manifest_version_id: "mv-package-batch-#{System.unique_integer([:positive])}"
      )

    {version, packages}
  end

  defp publish_manifest_request(version) do
    api_request(:post, "/api/orchestrator/v1/manifests", %{
      "manifest_version_id" => version.manifest_version_id,
      "content_hash" => version.content_hash,
      "schema_version" => version.schema_version,
      "runner_contract_version" => version.runner_contract_version,
      "runner_releases" => version.runner_releases,
      "serialization_format" => version.serialization_format,
      "manifest" => canonical_json(version.manifest)
    })
  end

  defp canonical_json(value) do
    value
    |> Favn.Manifest.Serializer.encode_manifest!()
    |> JSON.decode!()
  end

  defp authorize_platform_service_token do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      %{
        service_identity: "http-boundary",
        token: @service_token,
        enabled: true,
        platform_roles: [:platform_operator]
      }
    ])
  end

  defp api_request(method, path, body, opts \\ []) do
    conn =
      if is_nil(body) do
        Plug.Test.conn(method, path)
      else
        Plug.Test.conn(method, path, Jason.encode!(body))
        |> Plug.Conn.put_req_header("content-type", "application/json")
      end

    conn = Plug.Conn.put_req_header(conn, "authorization", "Bearer #{@service_token}")

    conn =
      case Keyword.get(opts, :fixture) do
        nil -> conn
        fixture -> Plug.Conn.put_req_header(conn, "x-favn-workspace-id", fixture.workspace_id)
      end

    conn =
      case Keyword.get(opts, :identity) do
        nil ->
          conn

        identity ->
          conn
          |> Plug.Conn.put_req_header("x-favn-actor-id", identity.actor.id)
          |> Plug.Conn.put_req_header("x-favn-session-token", identity.session.token)
      end

    conn =
      case Keyword.get(opts, :idempotency_key) do
        nil -> conn
        key -> Plug.Conn.put_req_header(conn, "idempotency-key", key)
      end

    Router.call(conn, Router.init([]))
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_storage_postgres, key)
  defp restore_env(key, value), do: Application.put_env(:favn_storage_postgres, key, value)

  defp collect_run_page_queries(acc) do
    receive do
      {:run_page_query, query} -> collect_run_page_queries([query | acc])
    after
      10 -> Enum.reverse(acc)
    end
  end

  defp query_row_count({:ok, %{num_rows: count}}), do: count
  defp query_row_count(%{num_rows: count}), do: count
  defp query_row_count(_result), do: :unknown

  defp capture_repo_queries(function) do
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn_storage_postgres, :repo, :query],
        fn _event, _measurements, metadata, pid ->
          send(pid, {:captured_repo_query, metadata.query})
        end,
        self()
      )

    try do
      result = function.()
      {result, collect_repo_queries([])}
    after
      :telemetry.detach(handler_id)
    end
  end

  defp collect_repo_queries(queries) do
    receive do
      {:captured_repo_query, query} -> collect_repo_queries([query | queries])
    after
      10 -> Enum.reverse(queries)
    end
  end

  defp share_repo_sandbox! do
    Sandbox.mode(Repo, {:shared, self()})
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
  end

  defp activation_body do
    %{
      "selection" => %{
        "common_assets" => "all",
        "common_pipelines" => "all",
        "workspace_assets" => [],
        "workspace_pipelines" => []
      },
      "configuration" => %{
        "secret_store_url" => "https://activation.vault.example.test",
        "resources" => %{"ducklake" => %{"secret_ref" => "ducklake-metadata"}}
      }
    }
  end

  defp continuation_regression_plan(evidence_generation_id) do
    ref = {MyApp.Asset, :asset}
    start_at = ~U[2026-07-01 00:00:00Z]

    windows =
      Enum.map(0..4, fn offset ->
        window_start = DateTime.add(start_at, offset, :day)
        anchor = Favn.Window.Key.new!(:day, window_start, "Etc/UTC")

        Favn.Window.Runtime.new!(
          :day,
          window_start,
          DateTime.add(window_start, 1, :day),
          anchor
        )
      end)

    [a_window, b_window, c_window, d_window, e_window] = windows

    keys = %{
      a: {ref, a_window.key},
      b: {ref, b_window.key},
      c: {ref, c_window.key},
      d: {ref, d_window.key},
      e: {ref, e_window.key}
    }

    pin = %{
      target_id: Favn.TargetIdentity.for_asset(ref),
      target_generation_id: nil,
      evidence_generation_id: evidence_generation_id,
      physical_relation: nil
    }

    node = fn key, window, upstream, downstream, stage, retry_policy ->
      Map.merge(pin, %{
        ref: ref,
        node_key: key,
        window: window,
        upstream: upstream,
        downstream: downstream,
        stage: stage,
        execution_pool: nil,
        action: :run,
        retry_policy: retry_policy,
        retry_policy_source: :asset,
        input_generations: if(upstream == [], do: [], else: [pin])
      })
    end

    default = Favn.Retry.Policy.default()
    retrying = Favn.Retry.Policy.new!(max_attempts: 2, backoff: 0)

    plan = %Favn.Plan{
      target_refs: [ref],
      target_node_keys: [keys.d, keys.e],
      dependencies: :all,
      nodes: %{
        keys.a => node.(keys.a, a_window, [], [], 0, default),
        keys.b => node.(keys.b, b_window, [], [keys.d], 0, retrying),
        keys.c => node.(keys.c, c_window, [], [keys.e], 0, default),
        keys.d => node.(keys.d, d_window, [keys.b], [], 1, default),
        keys.e => node.(keys.e, e_window, [keys.c], [], 1, default)
      },
      topo_order: [ref, ref, ref, ref, ref],
      stages: [[ref, ref, ref], [ref, ref]],
      node_stages: [[keys.a, keys.b, keys.c], [keys.d, keys.e]]
    }

    {plan, keys}
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_app_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
