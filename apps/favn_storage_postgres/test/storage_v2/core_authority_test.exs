defmodule FavnStoragePostgres.StorageV2.CoreAuthorityTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Manifest
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.Freshness.Policy
  alias Favn.SQL.Template
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
  alias FavnOrchestrator.Persistence.Commands.PurgePersistence
  alias FavnOrchestrator.Persistence.Commands.RegisterManifest
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
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
  alias FavnOrchestrator.Persistence.Commands.ReleaseExecutionLease
  alias FavnOrchestrator.Persistence.Commands.RenewExecutionLease
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetOperatorRunOverview
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.GetMaterializations
  alias FavnOrchestrator.Persistence.Queries.GetBackfill
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.Queries.GetDeploymentTargets
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Queries.PageLogs
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRunnerExecutions
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
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Operator.Catalogue
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunManager.SubmissionBuilder
  alias FavnOrchestrator.RunOwnership
  alias FavnOrchestrator.RunReadModel
  alias FavnOrchestrator.RunServer
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
  alias FavnStoragePostgres.Release
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.RuntimeInputKeyInventory
  alias FavnStoragePostgres.RunOwnership.Store, as: RunOwnershipStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore
  alias FavnStoragePostgres.Schemas.ManifestVersion, as: ManifestVersionRow
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
    assert runtime.required_runner_release_id == fixture.version.required_runner_release_id
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

  test "persists runner release identity and exposes it through manifest audit reads", fixture do
    row = Repo.get!(ManifestVersionRow, fixture.version.manifest_version_id)

    assert row.required_runner_release_id == fixture.version.required_runner_release_id

    {:ok, platform_context} =
      PlatformContext.new("release-auditor", "release-audit-grant", [:platform_reader])

    assert {:ok, page} =
             OperatorReadStore.page_manifests(%PageManifests{
               platform_context: platform_context,
               limit: 500
             })

    summary =
      Enum.find(page.items, &(&1.manifest_version_id == fixture.version.manifest_version_id))

    assert summary.required_runner_release_id == fixture.version.required_runner_release_id
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
                  runner_contract_version, required_runner_release_id,
                  payload_version, asset_count, pipeline_count, schedule_count,
                  atom_strings, manifest, inserted_at)
               VALUES ($1, $2, 9, 9, NULL, 1, 0, 0, 0, ARRAY[]::text[],
                       jsonb_build_object('assets', jsonb_build_array(),
                                          'pipelines', jsonb_build_array(),
                                          'schedules', jsonb_build_array()),
                       clock_timestamp())
               """,
               [manifest_version_id, content_hash]
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
    assert summary.required_runner_release_id == nil

    assert {:error,
            %{
              kind: :invalid,
              details: %{
                reason: :historical_manifest_not_activatable,
                schema_version: 9,
                current_schema_version: 10
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

  test "release-safe operations return redacted stable results and report upgrade blockers",
       fixture do
    telemetry_handler = "release-operation-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        telemetry_handler,
        [
          [:favn, :storage_postgres, :release_operation, :start],
          [:favn, :storage_postgres, :release_operation, :stop]
        ],
        fn event, measurements, metadata, _config ->
          send(parent, {event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> _ = :telemetry.detach(telemetry_handler) end)

    previous_keyring = System.get_env("FAVN_RUNTIME_INPUT_PIN_KEYS")
    previous_key_version = System.get_env("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION")

    System.put_env(
      "FAVN_RUNTIME_INPUT_PIN_KEYS",
      JSON.encode!(%{
        "1" => "0123456789abcdef0123456789abcdef",
        "2" => "abcdef0123456789abcdef0123456789"
      })
    )

    System.put_env("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", "2")

    on_exit(fn ->
      restore_system_env("FAVN_RUNTIME_INPUT_PIN_KEYS", previous_keyring)
      restore_system_env("FAVN_RUNTIME_INPUT_PIN_KEY_VERSION", previous_key_version)
    end)

    database_url = System.fetch_env!("FAVN_DATABASE_URL")

    log =
      capture_log(fn ->
        send(parent, {:release_result, Release.verify_schema()})
      end)

    assert_receive {:release_result,
                    {:ok,
                     %{
                       operation: :verify_schema,
                       status: :ok,
                       schema: "favn_control",
                       definition_fingerprint: fingerprint
                     }}}

    assert byte_size(fingerprint) == 64
    refute log =~ database_url

    if database_userinfo = URI.parse(database_url).userinfo do
      refute log =~ database_userinfo
    end

    assert_receive {[:favn, :storage_postgres, :release_operation, :start],
                    %{system_time: system_time}, %{operation: :verify_schema}}

    assert is_integer(system_time)

    assert_receive {[:favn, :storage_postgres, :release_operation, :stop],
                    %{duration_ms: duration_ms}, %{operation: :verify_schema, status: :ok}}

    assert is_integer(duration_ms) and duration_ms >= 0

    Repo.checkout(fn ->
      %{rows: [[previous_timeout]]} = SQL.query!(Repo, "SHOW statement_timeout", [])

      assert {:ok,
              %{
                operation: :verify_restore,
                status: :ok,
                statement_timeout_ms: 600_000
              }} = Release.verify_restore()

      assert %{rows: [[^previous_timeout]]} =
               SQL.query!(Repo, "SHOW statement_timeout", [])
    end)

    workspace_id = "release-ws-#{System.unique_integer([:positive])}"

    workspace = %{
      workspace_id: workspace_id,
      slug: workspace_id,
      display_name: "Release Workspace"
    }

    assert {:ok, %{operation: :provision_workspace, status: :ok, workspace_id: ^workspace_id}} =
             Release.provision_workspace(workspace)

    assert {:ok, %{operation: :provision_workspace, status: :ok, workspace_id: ^workspace_id}} =
             Release.provision_workspace(workspace)

    SQL.query!(
      Repo,
      """
      INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at)
      VALUES (98, clock_timestamp())
      ON CONFLICT (key_version) DO NOTHING
      """,
      []
    )

    assert {:ok,
            %{
              operation: :runtime_input_key_inventory,
              status: :ok,
              inventory: inventory,
              current_version: 2,
              retained_versions: [1, 2],
              invalid_versions: []
            }} = Release.runtime_input_key_inventory()

    assert Enum.any?(inventory, &(&1.key_version == 98 and &1.pin_count == 0))

    assert {:error,
            %{
              operation: :compact_runtime_input_keys,
              status: :error,
              code: :current_key_version_requested,
              current_version: 2
            }} = Release.compact_runtime_input_keys(2)

    assert_receive {[:favn, :storage_postgres, :release_operation, :start],
                    %{system_time: rejected_system_time},
                    %{operation: :compact_runtime_input_keys}}

    assert is_integer(rejected_system_time)

    assert_receive {[:favn, :storage_postgres, :release_operation, :stop],
                    %{duration_ms: rejected_duration_ms},
                    %{
                      operation: :compact_runtime_input_keys,
                      status: :error,
                      code: :current_key_version_requested
                    }}

    assert is_integer(rejected_duration_ms) and rejected_duration_ms >= 0

    assert {:ok,
            %{
              operation: :compact_runtime_input_keys,
              status: :ok,
              requested_versions: [98],
              removed_versions: [98]
            }} = Release.compact_runtime_input_keys(98)

    %{rows: [[current_role]]} = SQL.query!(Repo, "SELECT current_user", [])
    previous_role = System.get_env("FAVN_DATABASE_RUNTIME_ROLE")
    System.put_env("FAVN_DATABASE_RUNTIME_ROLE", current_role)

    on_exit(fn -> restore_system_env("FAVN_DATABASE_RUNTIME_ROLE", previous_role) end)

    assert {:error, %{operation: :migrate, status: :error, code: :restricted_runtime_role}} =
             Release.migrate()

    assert {:error, %{operation: :grant_runtime, status: :error, code: :restricted_runtime_role}} =
             Release.grant_runtime()

    assert_receive {[:favn, :storage_postgres, :release_operation, :stop],
                    %{duration_ms: failed_duration_ms},
                    %{
                      operation: :migrate,
                      status: :error,
                      code: :restricted_runtime_role
                    }}

    assert is_integer(failed_duration_ms) and failed_duration_ms >= 0
    :ok = :telemetry.detach(telemetry_handler)

    baseline = preflight_blockers()
    {run_command, legacy_run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(run_command)

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.manifest_versions
      SET schema_version = 9, runner_contract_version = 9,
          required_runner_release_id = NULL
      WHERE manifest_version_id = $1
      """,
      [fixture.version.manifest_version_id]
    )

    assert {:error,
            %{
              operation: :preflight_upgrade,
              status: :error,
              code: :runner_identity_upgrade_blocked,
              blocker_count: blocker_count,
              active_manifest_blocker_count: active_manifest_blocker_count,
              nonterminal_legacy_run_count: nonterminal_legacy_run_count,
              blocker_sample_limit: 100,
              truncated?: truncated?,
              active_legacy_manifests: manifest_blockers,
              nonterminal_legacy_runs: run_blockers
            }} = Release.preflight_upgrade()

    assert blocker_count == baseline.blocker_count + 2
    assert active_manifest_blocker_count == baseline.active_manifest_blocker_count + 1
    assert nonterminal_legacy_run_count == baseline.nonterminal_legacy_run_count + 1
    assert length(manifest_blockers) <= 100
    assert length(run_blockers) <= 100

    assert truncated? ==
             (active_manifest_blocker_count > length(manifest_blockers) or
                nonterminal_legacy_run_count > length(run_blockers))

    if not truncated? do
      assert Enum.any?(manifest_blockers, fn blocker ->
               blocker.workspace_id == fixture.workspace_id and
                 blocker.deployment_id == fixture.deployment_id and
                 blocker.manifest_version_id == fixture.version.manifest_version_id and
                 blocker.schema_version == 9
             end)

      assert Enum.any?(run_blockers, fn blocker ->
               blocker.workspace_id == fixture.workspace_id and blocker.run_id == legacy_run.id and
                 blocker.manifest_version_id == fixture.version.manifest_version_id and
                 blocker.schema_version == 9
             end)
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
    assert published_metadata.required_runner_release_id == version.required_runner_release_id
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

  test "readiness rejects a referenced runtime-input key version that is not retained" do
    SQL.query!(
      Repo,
      "INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at) VALUES (99, clock_timestamp())",
      []
    )

    assert {:ok, readiness} = Backend.readiness([])
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
    assert created.run.required_runner_release_id == fixture.version.required_runner_release_id
    assert created.event.sequence == 1

    assert %{rows: [[2, persisted_release_id]]} =
             SQL.query!(
               Repo,
               "SELECT snapshot_version, snapshot->>'required_runner_release_id' FROM favn_control.runs WHERE workspace_id = $1 AND run_id = $2",
               [fixture.workspace_id, run.id]
             )

    assert persisted_release_id == fixture.version.required_runner_release_id

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

  test "rejects a run whose runner release differs from its deployment manifest", fixture do
    {command, run} = create_run_command(fixture)
    alternate = FavnTestSupport.runner_release_id(:alternate)

    assert {:error,
            %{
              kind: :constraint,
              details: %{reason: :run_manifest_runner_release_mismatch}
            }} =
             RunStore.create_run(%{command | run: %{run | required_runner_release_id: alternate}})
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

  test "submission derives runner release identity only from the active manifest", fixture do
    caller_value = FavnTestSupport.runner_release_id(:alternate)

    assert {:ok, submission} =
             SubmissionBuilder.asset(fixture.workspace_context, {MyApp.Asset, :asset},
               required_runner_release_id: caller_value
             )

    assert submission.run_state.required_runner_release_id ==
             fixture.version.required_runner_release_id

    refute submission.run_state.required_runner_release_id == caller_value

    assert submission.event_metadata.required_runner_release_id ==
             fixture.version.required_runner_release_id
  end

  test "refuses persisted non-terminal legacy runs without a runner release binding", fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.runs
      SET snapshot_version = 1,
          snapshot = (snapshot - 'required_runner_release_id') ||
                     jsonb_build_object('format', 'favn.run_snapshot.storage.v2',
                                        'schema_version', 2)
      WHERE workspace_id = $1 AND run_id = $2
      """,
      [fixture.workspace_id, run.id]
    )

    assert {:error,
            %{
              kind: :conflict,
              details: %{reason: :legacy_runner_release_unbound}
            }} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })
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
    assert summary.required_runner_release_id == fixture.version.required_runner_release_id
    refute Map.has_key?(summary, :run)
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
      assert {:ok, %{items: [%{run_id: run_id}]}} =
               RunStore.page_run_summaries(%PageRuns{
                 scope: fixture.workspace_context,
                 limit: 1
               })

      assert run_id == run.id

      queries = collect_run_page_queries([])
      assert queries != []
      refute Enum.any?(queries, &Regex.match?(~r/\bsnapshot\b/i, &1))
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

    assert {:error, %{kind: :conflict}} =
             RunOwnershipStore.record_dispatch(%{
               dispatch
               | occurred_at: DateTime.add(dispatch.occurred_at, 1, :microsecond)
             })

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

  test "historical runner execution pages require an exact run id", fixture do
    assert {:error, %{kind: :invalid}} =
             RunOwnershipStore.page_executions(%PageRunnerExecutions{
               workspace_context: fixture.workspace_context,
               active_only?: false
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

    assert :ok = RunExecutionOwnership.complete_execution(active)
    assert {:ok, []} = RunExecutionOwnership.fetch_active(run)
  end

  test "recovery after runner restart terminalizes uncertain work without resubmitting",
       fixture do
    {command, run} = create_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    assert {:ok, authority} =
             RunOwnership.claim(fixture.workspace_context, run.id, "recovery:" <> run.id,
               command_id: "claim-recovery:" <> run.id
             )

    owned_run = RunState.with_storage_fence(run, authority.owner_id, authority.fencing_token)

    ledger =
      RunExecutionOwnership.new(owned_run,
        asset_step_id: "step:" <> run.id,
        asset_ref: run.asset_ref,
        attempt: 1,
        stage: 0
      )

    assert :ok = RunExecutionOwnership.persist(ledger)
    assert :ok = ledger |> RunExecutionOwnership.started() |> RunExecutionOwnership.persist()

    assert {:ok, %{active: [persisted_execution]}} =
             RunExecutionOwnership.recovery_evidence(owned_run)

    assert persisted_execution.status == :started

    configure_restarted_runner_client!()

    assert {:ok, pid} =
             RunServer.start_link(%{
               run_state: run,
               version: fixture.version,
               recovering?: true,
               storage_ownership: authority
             })

    monitor = Process.monitor(pid)
    assert_receive {:runner_cancelled_after_restart, execution_id}, 2_000
    assert execution_id == ledger.dispatch_id
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 2_000
    refute_receive {:runner_work_resubmitted, _work}, 100

    assert {:ok, failed} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })

    assert failed.status == :error
    assert failed.error["type"] == "uncertain_runner_recovery"

    assert %{rows: [["error"]]} =
             SQL.query!(
               Repo,
               """
               SELECT status
               FROM favn_control.runner_executions
               WHERE workspace_id = $1 AND runner_execution_id = $2
               """,
               [fixture.workspace_id, ledger.dispatch_id]
             )
  end

  test "corrupt pipeline retry checkpoints fail closed and release the runner manifest lease",
       fixture do
    {command, run} = pipeline_run_command(fixture)
    assert {:ok, _created} = RunStore.create_run(command)

    assert {:ok, authority} =
             RunOwnership.claim(fixture.workspace_context, run.id, "recovery:" <> run.id,
               command_id: "claim-corrupt-retry:" <> run.id
             )

    owned = RunState.with_storage_fence(run, authority.owner_id, authority.fencing_token)
    running = RunState.transition(owned, status: :running)

    assert :ok =
             TransitionWriter.persist_transition(
               fixture.workspace_context,
               running,
               :run_started,
               %{status: :running}
             )

    checkpoint_sequence = running.event_seq + 1

    retrying =
      RunState.transition(running,
        metadata:
          Map.merge(running.metadata, %{
            retrying: true,
            next_attempt: 2,
            next_retry_at: System.system_time(:millisecond),
            retry_state: %{
              kind: :pipeline,
              checkpoint_sequence: checkpoint_sequence,
              stage_index: 0,
              next_attempt: 2,
              stage: 0,
              next_retry_at: System.system_time(:millisecond)
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
                 retry_selection: %{
                   encoding: "stage_bitset_v1",
                   stage_size: 1,
                   retry_count: 1,
                   bits: ""
                 }
               }
             )

    configure_restarted_runner_client!()

    assert {:ok, pid} =
             RunServer.start_link(%{
               run_state: retrying,
               version: fixture.version,
               recovering?: true,
               storage_ownership: authority
             })

    monitor = Process.monitor(pid)
    assert_receive {:runner_manifest_acquired, lease_id}, 2_000
    assert_receive {:runner_manifest_released, ^lease_id}, 2_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 2_000

    assert {:ok, failed} =
             RunStore.get_run(%GetRun{
               workspace_context: fixture.workspace_context,
               run_id: run.id
             })

    assert failed.status == :error
  end

  test "cross-mode retry checkpoints terminalize recovery and release the manifest lease",
       fixture do
    configure_restarted_runner_client!()

    cases = [
      {:pipeline, &pipeline_run_command/1, &sequential_retry_checkpoint/1},
      {:sequential, &create_run_command/1, &pipeline_retry_checkpoint/1}
    ]

    for {mode, build_run, build_checkpoint} <- cases do
      {command, run} = build_run.(fixture)
      assert {:ok, _created} = RunStore.create_run(command)

      assert {:ok, authority} =
               RunOwnership.claim(fixture.workspace_context, run.id, "recovery:" <> run.id,
                 command_id: "claim-cross-mode-retry:" <> run.id
               )

      owned = RunState.with_storage_fence(run, authority.owner_id, authority.fencing_token)

      retrying =
        RunState.transition(owned,
          status: :running,
          metadata:
            Map.merge(owned.metadata, %{
              retrying: true,
              next_attempt: 2,
              retry_state: build_checkpoint.(owned)
            })
        )

      assert :ok =
               TransitionWriter.persist_transition(
                 fixture.workspace_context,
                 retrying,
                 :run_started,
                 %{status: :running}
               )

      assert {:ok, pid} =
               RunServer.start_link(%{
                 run_state: retrying,
                 version: fixture.version,
                 recovering?: true,
                 storage_ownership: authority
               })

      monitor = Process.monitor(pid)
      assert_receive {:runner_manifest_released, lease_id}, 2_000
      assert is_binary(lease_id) and lease_id != ""
      refute_receive {:runner_manifest_acquired, _lease_id}, 100
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 2_000

      assert {:ok, failed} =
               RunStore.get_run(%GetRun{
                 workspace_context: fixture.workspace_context,
                 run_id: run.id
               })

      assert failed.status == :error
      assert failed.error["type"] == "uncertain_runner_recovery"
      assert failed.error["reason"] =~ "invalid_retry_checkpoint"
      assert RunState.execution_mode(retrying) == mode
    end
  end

  @tag :slow
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
    {plan, keys} = continuation_regression_plan()
    {command, original} = pipeline_run_command(fixture)

    run =
      RunState.new(
        id: original.id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        required_runner_release_id: fixture.version.required_runner_release_id,
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

    configure_pipeline_runner_client!(runner_state)
    start_supervised!({FavnOrchestrator.ExecutionAdmission.Coordinator, []})

    assert {:ok, pid} = RunServer.start_link(%{run_state: run, version: fixture.version})
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, 5_000

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

    SQL.query!(
      Repo,
      "UPDATE favn_control.projection_cursors SET owner_id = NULL, claim_expires_at = NULL WHERE projector_name = 'control_plane_v1' AND shard_id = 0",
      []
    )

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

  test "service-token manifest activation is idempotent and durably audited", fixture do
    configure_release_runner_client!(fixture.version.required_runner_release_id)

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
               "required_runner_release_id" => required_runner_release_id
             }
           } = JSON.decode!(first.resp_body)

    assert required_runner_release_id == fixture.version.required_runner_release_id

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

    assert hd(matching_audits).detail["required_runner_release_id"] ==
             fixture.version.required_runner_release_id

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

  test "HTTP manifest activation rejects a different runner release without changing deployment",
       fixture do
    alternate = FavnTestSupport.runner_release_id(:alternate)
    configure_release_runner_client!(alternate)

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

    assert response.status == 409

    assert %{
             "error" => %{
               "code" => "runner_release_mismatch",
               "details" => %{
                 "required_runner_release_id" => required,
                 "runner_release_id" => ^alternate
               }
             }
           } = JSON.decode!(response.resp_body)

    assert required == fixture.version.required_runner_release_id

    assert_receive {:manifest_activation_rejected, %{count: 1}, activation_metadata}
    assert activation_metadata.status == :rejected
    assert activation_metadata.reason == :runner_release_mismatch
    assert activation_metadata.required_runner_release_id == required
    assert activation_metadata.runner_release_id == alternate

    assert {:ok, audit_page} = Identity.page_audit(fixture.workspace_context, limit: 20)

    rejected_audits =
      Enum.filter(
        audit_page.items,
        &(&1.action == "manifest.activate" and
            &1.subject_id == fixture.version.manifest_version_id and
            &1.detail["outcome"] == "rejected")
      )

    assert [rejected_audit] = rejected_audits
    assert rejected_audit.detail["rejection_reason"] == "runner_release_mismatch"
    assert rejected_audit.detail["required_runner_release_id"] == required
    assert rejected_audit.detail["runner_release_id"] == alternate
    refute Map.has_key?(rejected_audit.detail, "configuration")

    assert {:ok, %{manifest: active}} = Manifests.active(fixture.workspace_context)
    assert active.manifest_version_id == fixture.version.manifest_version_id
    assert active.required_runner_release_id == fixture.version.required_runner_release_id
  end

  test "HTTP manifest activation refuses an unavailable runner", fixture do
    configure_release_runner_client!(fixture.version.required_runner_release_id, ready?: false)

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

    assert response.status == 503
    assert %{"error" => %{"code" => "runner_unavailable"}} = JSON.decode!(response.resp_body)
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
               "run" => %{"required_runner_release_id" => required_runner_release_id}
             }
           } = JSON.decode!(available.resp_body)

    assert required_runner_release_id == fixture.version.required_runner_release_id

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
        "required_runner_release_id" => fixture.version.required_runner_release_id,
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
    assert Application.get_env(:favn_orchestrator, :auth_bootstrap_password) == nil

    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, password)
    Application.put_env(:favn_orchestrator, :workspace_ids, [second_workspace_id])
    assert :ok = Auth.bootstrap_configured_actor()
    assert Application.get_env(:favn_orchestrator, :auth_bootstrap_password) == nil

    Application.put_env(:favn_orchestrator, :auth_bootstrap_password, password)
    assert :ok = Auth.bootstrap_configured_actor()
    assert Application.get_env(:favn_orchestrator, :auth_bootstrap_password) == nil

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

    SQL.query!(
      Repo,
      "UPDATE favn_control.projection_cursors SET owner_id = NULL, claim_expires_at = NULL WHERE projector_name = 'control_plane_v1' AND shard_id = 0",
      []
    )

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
                 event_type: :step_started,
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

    assert compact.root_run.required_runner_release_id ==
             fixture.version.required_runner_release_id

    assert Enum.all?(compact.runs, fn summary ->
             summary.required_runner_release_id == fixture.version.required_runner_release_id
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

    completed = RunState.transition(progressed, status: :ok)

    assert {:ok, _committed} =
             RunStore.commit_transition(%CommitRunTransition{
               workspace_context: fixture.workspace_context,
               command_id: "project-step-finished:" <> run.id,
               expected_sequence: 3,
               run: completed,
               event: %{
                 run_id: run.id,
                 sequence: 4,
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
    assert target_run.required_runner_release_id == fixture.version.required_runner_release_id

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

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.projection_cursors
      SET owner_id = NULL, claim_expires_at = NULL
      WHERE projector_name = 'control_plane_v1' AND shard_id = 0
      """,
      []
    )

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

  defp create_run_command(fixture, run_id \\ nil) do
    run_id = run_id || "run-#{System.unique_integer([:positive])}"

    run =
      RunState.new(
        id: run_id,
        workspace_id: fixture.workspace_id,
        deployment_id: fixture.deployment_id,
        manifest_version_id: fixture.version.manifest_version_id,
        manifest_content_hash: fixture.version.content_hash,
        required_runner_release_id: fixture.version.required_runner_release_id,
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
        required_runner_release_id: fixture.version.required_runner_release_id,
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
          selectors: [{:asset, {MyApp.Asset, :asset}}]
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
      "required_runner_release_id" => version.required_runner_release_id,
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

  defp configure_restarted_runner_client! do
    env_keys = [:runtime_config_dynamic_env?, :runner_client, :runner_client_opts]
    previous = Map.new(env_keys, &{&1, Application.get_env(:favn_orchestrator, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_app_env(key, value) end) end)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)

    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      FavnStoragePostgres.TestRestartedRunnerClient
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      test_pid: self(),
      runner_release_id: FavnTestSupport.runner_release_id()
    )

    Sandbox.mode(Repo, {:shared, self()})
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
  end

  defp configure_pipeline_runner_client!(runner_state) do
    env_keys = [:runtime_config_dynamic_env?, :runner_client, :runner_client_opts]
    previous = Map.new(env_keys, &{&1, Application.get_env(:favn_orchestrator, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_app_env(key, value) end) end)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)

    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      FavnStoragePostgres.TestPipelineRunnerClient
    )

    Application.put_env(:favn_orchestrator, :runner_client_opts,
      state: runner_state,
      runner_release_id: FavnTestSupport.runner_release_id()
    )

    Sandbox.mode(Repo, {:shared, self()})
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
  end

  defp configure_release_runner_client!(runner_release_id, runner_opts \\ []) do
    env_keys = [:runtime_config_dynamic_env?, :runner_client, :runner_client_opts]
    previous = Map.new(env_keys, &{&1, Application.get_env(:favn_orchestrator, &1)})
    on_exit(fn -> Enum.each(previous, fn {key, value} -> restore_app_env(key, value) end) end)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)

    Application.put_env(
      :favn_orchestrator,
      :runner_client,
      FavnStoragePostgres.TestReleaseRunnerClient
    )

    Application.put_env(
      :favn_orchestrator,
      :runner_client_opts,
      Keyword.put(runner_opts, :runner_release_id, runner_release_id)
    )
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

  defp continuation_regression_plan do
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

    node = fn key, window, upstream, downstream, stage, retry_policy ->
      %{
        ref: ref,
        node_key: key,
        window: window,
        upstream: upstream,
        downstream: downstream,
        stage: stage,
        execution_pool: nil,
        action: :run,
        retry_policy: retry_policy,
        retry_policy_source: :asset
      }
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

  defp sequential_retry_checkpoint(%RunState{} = run) do
    %{
      kind: :sequential,
      sequential_index: 0,
      next_retry_at: System.system_time(:millisecond),
      retry: %{
        asset_ref: run.asset_ref,
        node_key: {run.asset_ref, nil},
        asset_step_id: "cross-mode-step:" <> run.id,
        stage: 0,
        next_attempt: 2,
        retry_after_ms: 0
      }
    }
  end

  defp pipeline_retry_checkpoint(%RunState{} = run) do
    %{
      kind: :pipeline,
      checkpoint_sequence: run.event_seq + 1,
      stage_index: 0,
      next_attempt: 2,
      stage: 0,
      next_retry_at: System.system_time(:millisecond)
    }
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_app_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)

  defp preflight_blockers do
    case Release.preflight_upgrade() do
      {:ok, blockers} -> blockers
      {:error, blockers} -> blockers
    end
  end

  defp restore_system_env(key, nil), do: System.delete_env(key)
  defp restore_system_env(key, value), do: System.put_env(key, value)
end

defmodule FavnStoragePostgres.TestReleaseRunnerClient do
  @behaviour Favn.Contracts.RunnerClient

  @impl true
  def register_manifest(_version, _opts), do: :ok

  @impl true
  def ensure_manifest(_version, _opts), do: :ok

  @impl true
  def acquire_manifest(_version, _lease_id, _expires_at, _planned_asset_refs, _opts), do: :ok

  @impl true
  def renew_manifest(_lease_id, _expires_at, _opts), do: :ok

  @impl true
  def release_manifest(_lease_id, _opts), do: :ok

  @impl true
  def submit_work(_work, _opts), do: {:error, :unsupported}

  @impl true
  def await_result(_execution_id, _timeout, _opts), do: {:error, :unsupported}

  @impl true
  def cancel_work(_execution_id, _reason, _opts), do: {:error, :unsupported}

  @impl true
  def inspect_relation(_request, _opts), do: {:error, :unsupported}

  @impl true
  def diagnostics(opts) do
    {:ok,
     %{
       available?: true,
       ready?: Keyword.get(opts, :ready?, true),
       status: if(Keyword.get(opts, :ready?, true), do: :ready, else: :not_ready),
       self_verified?: true,
       runner_release_id: Keyword.fetch!(opts, :runner_release_id),
       favn_version: Favn.RunnerRelease.current_favn_version(),
       runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
       node_name: "runner@runner.internal"
     }}
  end
end

defmodule FavnStoragePostgres.TestRestartedRunnerClient do
  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RunnerCancellation

  @impl true
  def register_manifest(_version, _opts), do: :ok

  @impl true
  def ensure_manifest(_version, _opts), do: :ok

  @impl true
  def acquire_manifest(_version, lease_id, _expires_at, _planned_asset_refs, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:runner_manifest_acquired, lease_id})
    :ok
  end

  @impl true
  def renew_manifest(_lease_id, _expires_at, _opts), do: :ok

  @impl true
  def release_manifest(lease_id, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:runner_manifest_released, lease_id})
    :ok
  end

  @impl true
  def submit_work(work, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:runner_work_resubmitted, work})
    {:error, :unexpected_resubmission}
  end

  @impl true
  def await_result(_execution_id, _timeout, _opts), do: {:error, :execution_not_found}

  @impl true
  def cancel_work(execution_id, _reason, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:runner_cancelled_after_restart, execution_id})

    {:ok,
     RunnerCancellation.outcome(:not_found,
       execution_id: execution_id,
       runner_status: :not_found,
       native_status: :native_cancel_unknown
     )}
  end

  @impl true
  def inspect_relation(_request, _opts), do: {:error, :unsupported}

  @impl true
  def diagnostics(opts) do
    {:ok,
     %{
       available?: true,
       ready?: true,
       status: :ready,
       self_verified?: true,
       runner_release_id: Keyword.fetch!(opts, :runner_release_id),
       favn_version: Favn.RunnerRelease.current_favn_version(),
       runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
       node_name: "runner@runner.internal"
     }}
  end
end

defmodule FavnStoragePostgres.TestPipelineRunnerClient do
  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork

  @impl true
  def register_manifest(_version, _opts), do: :ok

  @impl true
  def ensure_manifest(_version, _opts), do: :ok

  @impl true
  def acquire_manifest(_version, _lease_id, _expires_at, _planned_asset_refs, _opts), do: :ok

  @impl true
  def renew_manifest(_lease_id, _expires_at, _opts), do: :ok

  @impl true
  def release_manifest(_lease_id, _opts), do: :ok

  @impl true
  def resolve_runtime_inputs(_work, _opts), do: {:ok, nil}

  @impl true
  def submit_work(%RunnerWork{} = work, opts) do
    state = Keyword.fetch!(opts, :state)
    node_key = RunnerWork.node_key(work)

    Agent.update(state, fn current ->
      %{
        current
        | work: Map.put(current.work, work.execution_id, work),
          submitted: [node_key | current.submitted]
      }
    end)

    {:ok, work.execution_id}
  end

  @impl true
  def await_result(execution_id, _timeout, opts) do
    state = Keyword.fetch!(opts, :state)
    %{work: work, fail_key: fail_key} = Agent.get(state, &%{work: &1.work, fail_key: &1.fail_key})
    work = Map.fetch!(work, execution_id)

    if RunnerWork.node_key(work) == fail_key do
      retryable? = work.attempt == 1

      error =
        RunnerError.new(
          type: :fixture_failure,
          message: "fixture failure",
          retryable?: retryable?,
          outcome: if(retryable?, do: :safe_failure, else: :unknown)
        )

      {:ok, result(work, :error, error)}
    else
      {:ok, result(work, :ok, nil)}
    end
  end

  @impl true
  def cancel_work(execution_id, _reason, _opts) do
    {:ok,
     RunnerCancellation.outcome(:acknowledged,
       execution_id: execution_id,
       runner_status: :cancelled,
       native_status: :native_cancel_unknown
     )}
  end

  @impl true
  def inspect_relation(_request, _opts), do: {:error, :unsupported}

  @impl true
  def diagnostics(opts) do
    {:ok,
     %{
       available?: true,
       ready?: true,
       status: :ready,
       self_verified?: true,
       runner_release_id: Keyword.fetch!(opts, :runner_release_id),
       favn_version: Favn.RunnerRelease.current_favn_version(),
       runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
       node_name: "runner@runner.internal"
     }}
  end

  defp result(work, status, error) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: status,
      asset_results: [],
      error: error,
      metadata: %{}
    }
  end
end
