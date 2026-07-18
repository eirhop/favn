defmodule FavnStoragePostgres.StorageV2.CoreAuthorityTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Persistence.BackfillPlan
  alias FavnOrchestrator.Persistence.Commands.ActivateBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.BackfillMissingProjection
  alias FavnOrchestrator.Persistence.Commands.AppendBackfillPlanBatch
  alias FavnOrchestrator.Persistence.Commands.BackfillPlanWindow
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.AdvanceRunnerExecution
  alias FavnOrchestrator.Persistence.Commands.ClaimRun
  alias FavnOrchestrator.Persistence.Commands.ClaimMaterialization
  alias FavnOrchestrator.Persistence.Commands.ClaimBackfillWindows
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.DeployManifest
  alias FavnOrchestrator.Persistence.Commands.DeploymentTarget
  alias FavnOrchestrator.Persistence.Commands.DeploymentSchedule
  alias FavnOrchestrator.Persistence.Commands.DeploymentCapacityScope
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RecordRunnerDispatch
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Commands.ReleaseRunOwnership
  alias FavnOrchestrator.Persistence.Commands.FinishMaterialization
  alias FavnOrchestrator.Persistence.Commands.AppendLogBatch
  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.LogEntry
  alias FavnOrchestrator.Persistence.Commands.PurgeLogs
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.RenewMaterializationClaim
  alias FavnOrchestrator.Persistence.Commands.RenewRunOwnership
  alias FavnOrchestrator.Persistence.Commands.ClaimDueSchedules
  alias FavnOrchestrator.Persistence.Commands.ClaimScheduleOccurrences
  alias FavnOrchestrator.Persistence.Commands.CommitScheduleEvaluation
  alias FavnOrchestrator.Persistence.Commands.CompleteScheduleOccurrence
  alias FavnOrchestrator.Persistence.Commands.ScheduleOccurrenceIntent
  alias FavnOrchestrator.Persistence.Commands.StartBackfillPlan
  alias FavnOrchestrator.Persistence.Commands.TransitionBackfillWindow
  alias FavnOrchestrator.Persistence.Commands.RunTarget
  alias FavnOrchestrator.Persistence.Commands.AdmitExecution
  alias FavnOrchestrator.Persistence.Commands.CapacityRequest
  alias FavnOrchestrator.Persistence.Commands.ClaimAdmissionWaiters
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.RenewExecutionLease
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.GetMaterializations
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Queries.PageBackfillWindows
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.Queries.PageScheduleOccurrences
  alias FavnOrchestrator.Persistence.Queries.PageSchedules
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.Selectors.ActorByUsername
  alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunCancellation
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Identity
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunOwnership
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
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RunOwnership.Store, as: RunOwnershipStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.Schemas.RuntimeInputPin, as: RuntimeInputPinRow
  alias FavnStoragePostgres.Scheduler.Store, as: SchedulerStore
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

  test "registers and deploys an immutable exact manifest catalog", fixture do
    assert {:ok, runtime} =
             RegistryStore.get_runtime_state(%GetRuntimeState{
               workspace_context: fixture.workspace_context
             })

    assert runtime.workspace_id == fixture.workspace_id
    assert runtime.deployment_id == fixture.deployment_id
    assert runtime.manifest_version_id == fixture.version.manifest_version_id
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

  test "pipeline backfill planning persists an exact resumable V2 plan", fixture do
    range = %{
      "kind" => "day",
      "from" => "2026-07-01",
      "to" => "2026-07-03",
      "timezone" => "Etc/UTC"
    }

    assert {:ok, %{window_count: 3, target_id: target_id}} =
             Backfills.plan_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range
             )

    assert target_id == fixture.pipeline_target_id

    assert {:ok, backfill} =
             Backfills.submit_pipeline(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.pipeline_target_id,
               range,
               root_run_id: "run-backfill-#{System.unique_integer([:positive])}"
             )

    assert backfill.status == :ready
    assert backfill.expected_window_count == 3
    assert backfill.appended_window_count == 3

    assert {:ok, page} =
             Backfills.page_windows(fixture.workspace_context, backfill.backfill_id, limit: 2)

    assert length(page.items) == 2
    assert page.has_more?
    assert Enum.all?(page.items, &(&1.status == :ready))
  end

  test "asset backfills use the same resumable V2 ledger", fixture do
    range = %{
      "kind" => "day",
      "from" => "2026-07-01",
      "to" => "2026-07-02",
      "timezone" => "Etc/UTC"
    }

    assert {:ok, %{window_count: 2, target_id: target_id}} =
             Backfills.plan_asset(
               fixture.workspace_context,
               fixture.version.manifest_version_id,
               fixture.target_id,
               range,
               dependencies: :none
             )

    assert target_id == fixture.target_id

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

    assert {:ok, %RunState{submit_kind: :backfill_asset, target_refs: [{MyApp.Asset, :asset}]}} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: backfill.root_run_id
             })

    assert {:ok, page} =
             Backfills.page_windows(fixture.workspace_context, backfill.backfill_id, limit: 10)

    assert length(page.items) == 2
    assert Enum.all?(page.items, &(&1.status == :ready))
  end

  test "reports exact backend capabilities and schema readiness" do
    assert :ok = Backend.stores() |> Stores.validate()
    assert {:ok, %{ready?: true, status: :ready}} = Backend.readiness([])

    assert {:ok, diagnostics} =
             Backend.diagnostics(
               url: System.fetch_env!("FAVN_DATABASE_URL"),
               ssl_mode: :disable
             )

    assert diagnostics.engine.name == :postgresql
    assert diagnostics.engine.version.major == 18
    assert diagnostics.schema.ready?
    assert diagnostics.metadata.runtime_input_keys.configured?
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
            customer_visible: true
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
    assert created.event.sequence == 1

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

  test "operator run pages preserve decoded scope and metadata", fixture do
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

    assert summary.run.params == %{
             "window" => %{
               "kind" => "day",
               "value" => "2026-07-16",
               "timezone" => "Etc/UTC"
             }
           }

    assert summary.run.metadata["request_source"] == "operator-test"
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
          customer_visible: true
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
        params: %{account_id: 42, token: "must-remain-encrypted"},
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

    conflicting =
      pin
      |> Map.put(:params, %{account_id: 43, token: "different"})
      |> Map.put(:payload_fingerprint, "different")

    assert {:error, %{kind: :conflict}} =
             RunStore.pin_runtime_inputs(%{pin_command | pins: [conflicting]})
  end

  test "fences run ownership and durable runner execution", fixture do
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

    dispatch = %RecordRunnerDispatch{
      workspace_context: fixture.workspace_context,
      command_id: "dispatch:" <> run.id,
      run_id: run.id,
      runner_execution_id: "execution:" <> run.id,
      dispatch_id: "dispatch-id:" <> run.id,
      owner_id: "node-a",
      fencing_token: ownership.fencing_token,
      payload: %{"plan_id" => "plan-one"},
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, execution} = RunOwnershipStore.record_dispatch(dispatch)
    assert execution.status == :dispatching
    assert {:ok, ^execution} = RunOwnershipStore.record_dispatch(dispatch)

    advance = %AdvanceRunnerExecution{
      workspace_context: fixture.workspace_context,
      command_id: "execution-running:" <> run.id,
      run_id: run.id,
      runner_execution_id: dispatch.runner_execution_id,
      owner_id: "node-a",
      fencing_token: ownership.fencing_token,
      expected_version: 1,
      status: :running,
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, running} = RunOwnershipStore.advance_execution(advance)
    assert running.status == :running
    assert running.version == 2
    assert {:ok, ^running} = RunOwnershipStore.advance_execution(advance)

    assert :ok =
             RunOwnershipStore.release_run(%ReleaseRunOwnership{
               workspace_context: fixture.workspace_context,
               run_id: run.id,
               owner_id: "node-a",
               fencing_token: ownership.fencing_token
             })

    assert {:error, %{kind: :fenced}} =
             RunOwnershipStore.advance_execution(%{
               advance
               | command_id: "execution-finished:" <> run.id,
                 expected_version: 2,
                 status: :ok,
                 result: %{}
             })
  end

  test "orchestrator runner ledger uses preallocated ids and the current run fence", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    owner_id = "orchestrator:" <> run.id

    assert {:ok, authority} =
             RunOwnership.claim(fixture.workspace_context, run.id, owner_id,
               command_id: "claim-orchestrator-ledger:" <> run.id
             )

    run = RunState.with_storage_fence(run, authority.owner_id, authority.fencing_token)

    ledger =
      RunExecutionOwnership.new(run,
        asset_step_id: "step:" <> run.id,
        asset_ref: run.asset_ref,
        attempt: 1,
        stage: 0
      )

    assert :ok = RunExecutionOwnership.persist(ledger)

    submitted = RunExecutionOwnership.submitted(ledger, ledger.dispatch_id)
    assert :ok = RunExecutionOwnership.persist(submitted)
    assert :ok = submitted |> RunExecutionOwnership.started() |> RunExecutionOwnership.persist()

    assert {:ok, [active]} = RunExecutionOwnership.fetch_active(run)
    assert active.runner_execution_id == ledger.dispatch_id
    assert active.status == :started
    assert active.persistence_version == 2

    assert :ok = RunExecutionOwnership.complete_execution(run, ledger.dispatch_id)
    assert {:ok, []} = RunExecutionOwnership.fetch_active(run)
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
    assert operator_schedule.activation_state == :enabled

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

    {run_command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    completion = %CompleteScheduleOccurrence{
      workspace_context: fixture.workspace_context,
      command_id: "occurrence-complete:" <> fixture.workspace_id,
      occurrence_id: occurrence_id,
      owner_id: "scheduler-a",
      claim_generation: claimed.claim_generation,
      run_id: run.id,
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, completed} = SchedulerStore.complete_occurrence(completion)
    assert completed.status == :completed
    assert completed.run_id == run.id
    assert {:ok, ^completed} = SchedulerStore.complete_occurrence(completion)

    %{rows: replay_rows} =
      SQL.query!(
        Repo,
        "SELECT status, claim_owner, claim_command_id FROM favn_control.schedule_occurrences WHERE workspace_id = $1 AND occurrence_id = $2",
        [fixture.workspace_id, occurrence_id]
      )

    assert replay_rows == [["completed", "scheduler-a", occurrence_claim.batch_id]]

    assert {:error, %{kind: :fenced}} =
             SchedulerStore.claim_occurrences(occurrence_claim)
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

    assert {:ok, [claimed]} =
             AdmissionStore.claim_waiters(%ClaimAdmissionWaiters{
               workspace_context: fixture.workspace_context,
               batch_id: "waiter-claim:" <> fixture.workspace_id,
               scope_id: fixture.capacity_scope_id,
               owner_id: "dispatcher-a",
               lease_duration_ms: 30_000,
               limit: 10
             })

    assert claimed.waiter_id == second.waiter_id
    assert claimed.status == :claimed

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
      partition_key: "2026-07-17",
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

    node_key_fingerprint = String.duplicate("ab", 32)

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

    SQL.query!(
      Repo,
      "UPDATE favn_control.projection_cursors SET owner_id = NULL, claim_expires_at = NULL WHERE projector_name = 'control_plane_v1' AND shard_id = 0",
      []
    )

    assert {:ok, %{count: count}} = Projector.project_batch("projector:" <> run.id)
    assert count > 0

    assert %{rows: [[persisted_node_key_hash]]} =
             SQL.query!(
               Repo,
               "SELECT latest_success_node_key_hash FROM favn_control.asset_freshness_states WHERE workspace_id = $1 AND latest_success_materialization_id = $2",
               [fixture.workspace_id, finish.materialization_id]
             )

    assert persisted_node_key_hash == Base.decode16!(node_key_fingerprint, case: :mixed)
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
               filter_kind: :level,
               filter_value: :error,
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
             Identity.change_password(fixture.workspace_context, actor.id, "replacement-password")

    assert {:ok, self_context} =
             WorkspaceContext.new(fixture.workspace_id, actor.id, [:customer_operator])

    assert :ok = Identity.change_password(self_context, actor.id, "replacement-password")

    assert {:error, :invalid_credentials} =
             Identity.authenticate_password(login_context, username, password)

    assert {:ok, _authenticated} =
             Identity.authenticate_password(login_context, username, "replacement-password")

    assert {:ok, issued} = Identity.issue_session(login_context, actor.id)
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

  test "HTTP manifest activation is idempotent and writes one durable audit record", fixture do
    identity = api_identity(fixture, [:admin])

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
        identity: identity,
        idempotency_key: "activate-once"
      )

    replay =
      api_request(:post, path, body,
        fixture: fixture,
        identity: identity,
        idempotency_key: "activate-once"
      )

    assert first.status == 200
    assert replay.status == 200
    assert replay.resp_body == first.resp_body

    assert {:ok, audit_page} = Identity.page_audit(fixture.workspace_context, limit: 20)

    matching_audits =
      Enum.filter(
        audit_page.items,
        &(&1.action == "manifest.activate" and
            &1.subject_id == fixture.version.manifest_version_id)
      )

    assert length(matching_audits) == 1

    assert %{rows: [[1]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.idempotency_records WHERE workspace_id = $1 AND operation = 'manifest.activate'",
               [fixture.workspace_id]
             )
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
        "serialization_format" => fixture.version.serialization_format
      })

    assert publish.status == 403
  end

  test "one bootstrap identity can be added idempotently to separate runtime workspaces",
       fixture do
    second_workspace_id = "ws-bootstrap-#{System.unique_integer([:positive])}"

    assert :ok =
             RegistryStore.provision_workspace(%ProvisionWorkspace{
               platform_context: fixture.platform_context,
               workspace_id: second_workspace_id,
               slug: second_workspace_id,
               display_name: "Second bootstrap workspace",
               occurred_at: DateTime.utc_now()
             })

    keys = [
      :auth_bootstrap_username,
      :auth_bootstrap_password,
      :auth_bootstrap_display_name,
      :auth_bootstrap_roles,
      :workspace_ids
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:favn_orchestrator, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_app_env(key, value) end) end)

    username = "bootstrap-#{System.unique_integer([:positive])}"
    password = "bootstrap-password-long"

    Application.put_env(:favn_orchestrator, :auth_bootstrap_username, username)
    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, password)
    Application.put_env(:favn_orchestrator, :auth_bootstrap_display_name, "Platform Operator")
    Application.put_env(:favn_orchestrator, :auth_bootstrap_roles, [:admin])

    Application.put_env(:favn_orchestrator, :workspace_ids, [fixture.workspace_id])
    assert :ok = Auth.bootstrap_configured_actor()

    Application.put_env(:favn_orchestrator, :workspace_ids, [second_workspace_id])
    assert :ok = Auth.bootstrap_configured_actor()
    assert :ok = Auth.bootstrap_configured_actor()

    {:ok, first_login_context} =
      WorkspaceContext.new(fixture.workspace_id, "auth:password", [:customer_reader])

    {:ok, second_login_context} =
      WorkspaceContext.new(second_workspace_id, "auth:password", [:customer_reader])

    assert {:ok, first_actor} =
             Identity.authenticate_password(first_login_context, username, password)

    assert {:ok, second_actor} =
             Identity.authenticate_password(second_login_context, username, password)

    assert first_actor.id == second_actor.id
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

    version = version || manifest_version("mv_#{unique}")

    if is_nil(version) do
      raise "manifest version is required"
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
            customer_visible: true
          },
          %DeploymentTarget{
            target_kind: :pipeline,
            target_id: pipeline_target_id,
            selection_source: :common,
            customer_visible: true
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

  defp create_run_command(fixture, run_id \\ nil) do
    run_id = run_id || "run-#{System.unique_integer([:positive])}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
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

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{
          ref: {MyApp.Asset, :asset},
          module: MyApp.Asset,
          name: :asset,
          sql_execution: %Favn.Manifest.SQLExecution{
            sql: "select 1",
            template: nil,
            runtime_inputs: %Favn.RuntimeInputResolver.Ref{
              module: MyApp.RuntimeInputResolver
            }
          }
        },
        %Favn.Manifest.Asset{
          ref: {MyApp.PrivateAsset, :private},
          module: MyApp.PrivateAsset,
          name: :private
        }
      ],
      pipelines: [
        %Favn.Manifest.Pipeline{
          module: MyApp.Pipeline,
          name: :daily,
          selectors: [{:asset, {MyApp.Asset, :asset}}]
        }
      ]
    }

    {:ok, version} =
      Version.new(FavnTestSupport.with_manifest_graph(manifest),
        manifest_version_id: manifest_version_id
      )

    version
  end

  defp drain_projector(owner_id, total \\ 0) do
    case Projector.project_batch(owner_id, limit: 250) do
      {:ok, %{count: 250}} -> drain_projector(owner_id, total + 250)
      {:ok, %{count: count}} -> total + count
    end
  end

  defp api_identity(fixture, roles) do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    username = "http-actor-#{suffix}"

    assert {:ok, actor} =
             Identity.create_actor(
               fixture.workspace_context,
               username,
               "http-boundary-password-#{suffix}",
               "HTTP Boundary Actor",
               roles
             )

    assert {:ok, login_context} =
             WorkspaceContext.new(fixture.workspace_id, "auth:http-test", [:customer_reader])

    assert {:ok, session} = Identity.issue_session(login_context, actor.id)
    %{actor: actor, session: session}
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

  defp restore_app_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_app_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
