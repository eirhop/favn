defmodule FavnStoragePostgres.StorageV2.ManifestDeploymentsTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test
  import ExUnit.CaptureLog

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerTask.ClaimRequest
  alias Favn.Contracts.RunnerTask.Registration
  alias Favn.Contracts.RunnerTask.Result
  alias Favn.Contracts.RunnerTask.Started
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Template
  alias FavnAuthoring.Deployment.ManifestArchive
  alias FavnAuthoring.Deployment.ManifestBuilder
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.ManifestDeploymentDispatcher
  alias FavnOrchestrator.ManifestActivationDiagnostics
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.API.ManifestDeployment
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth.ManifestDeployerTokens
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.OperationRunnerTasks
  alias FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.BeginManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetManifestDeployment
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunnerDemandLimiter
  alias FavnOrchestrator.RunnerQueueSupervisor
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTaskResultRouter
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.TargetCompatibilityPlanner
  alias FavnStoragePostgres.Backend
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  @capacity_token "7b0a9d3f8e2c4615a794b6d1038fce254ec1b73a860d924f11c7e5a098bd6230"

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} =
      Config.repo_options(url: url, ssl_mode: :disable, pool: Sandbox, pool_size: 2)

    start_supervised!({Repo, options})
    start_supervised!({Lifecycle, shutdown_drain_timeout_ms: 120_000})
    start_memory_capacity_if_needed()
    :ok = Lifecycle.mark_accepting()
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
    :ok
  end

  defp start_memory_capacity_if_needed do
    unless Process.whereis(MemoryCapacity.Coordinator) do
      start_supervised!(
        {MemoryCapacity.Supervisor,
         provider_opts: [
           ceiling_bytes: Application.fetch_env!(:favn_orchestrator, :memory_ceiling_bytes)
         ]}
      )
    end
  end

  setup do
    :ok = Sandbox.checkout(Repo)
    unique = Integer.to_string(System.unique_integer([:positive]))
    workspace_id = "manifest-deployments-#{unique}"

    {:ok, platform_context} =
      PlatformContext.new("manifest-test", "manifest-test-#{unique}", [:platform_admin])

    :ok =
      Store.provision_workspace(%ProvisionWorkspace{
        platform_context: platform_context,
        workspace_id: workspace_id,
        slug: "manifest-deployments-#{unique}",
        display_name: "Manifest deployments #{unique}",
        occurred_at: DateTime.utc_now()
      })

    workspace_context =
      SystemContext.workspace(workspace_id, :manifest_test, roles: [:platform_operator])

    {:ok, deployment_context} =
      ManifestDeploymentContext.new("ci-v1", workspace_id, "request-#{unique}")

    raw_token = "7b0a9d3f8e2c4615a794b6d1038fce25"

    {:ok, manifest_deployer_tokens} =
      ManifestDeployerTokens.from_env_string(
        Jason.encode!([
          %{
            "service_identity" => "ci-v1",
            "workspace_ids" => [workspace_id],
            "token" => raw_token
          }
        ])
      )

    previous_tokens = Application.get_env(:favn_orchestrator, :manifest_deployer_tokens)
    Application.put_env(:favn_orchestrator, :manifest_deployer_tokens, manifest_deployer_tokens)

    on_exit(fn -> restore_env(:manifest_deployer_tokens, previous_tokens) end)

    ref = {MyApp.ManifestDeploymentAsset, :asset}
    package = execution_package(ref)

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        relation:
          RelationRef.new!(connection: :warehouse, schema: "manifest", name: "deployment"),
        materialization: :table,
        execution_package_hash: package.content_hash
      })

    manifest =
      %Manifest{assets: [asset]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} = Version.new(manifest)

    :ok = ExecutionPackages.register(platform_context, [package])

    %{
      deployment_context: deployment_context,
      platform_context: platform_context,
      package: package,
      version: version,
      workspace_context: workspace_context,
      workspace_id: workspace_id,
      raw_token: raw_token
    }
  end

  test "upload admission is distributed, bounded, and explicitly released", context do
    now = DateTime.utc_now()

    second_workspace = provision_workspace(context, "upload-second")
    third_workspace = provision_workspace(context, "upload-third")

    {:ok, same_identity_other_workspace} =
      ManifestDeploymentContext.new("ci-v1", second_workspace, "same-identity-request")

    {:ok, other_identity_same_workspace} =
      ManifestDeploymentContext.new("other-ci-v1", context.workspace_id, "same-workspace-request")

    {:ok, second_context} =
      ManifestDeploymentContext.new("second-ci-v1", second_workspace, "second-request")

    {:ok, third_context} =
      ManifestDeploymentContext.new("third-ci-v1", third_workspace, "third-request")

    acquire = %AcquireManifestUploadLease{
      context: context.deployment_context,
      lease_id: "upload-one",
      occurred_at: now,
      expires_at: DateTime.add(now, 60, :second)
    }

    assert :ok = Store.acquire_manifest_upload_lease(acquire)

    assert {:error, %{kind: :limit_exceeded, details: %{reason: :deployment_upload_busy}}} =
             Store.acquire_manifest_upload_lease(%{
               acquire
               | context: same_identity_other_workspace,
                 lease_id: "same-identity"
             })

    assert {:error, %{kind: :limit_exceeded, details: %{reason: :deployment_upload_busy}}} =
             Store.acquire_manifest_upload_lease(%{
               acquire
               | context: other_identity_same_workspace,
                 lease_id: "same-workspace"
             })

    assert :ok =
             Store.acquire_manifest_upload_lease(%{
               acquire
               | context: second_context,
                 lease_id: "upload-two"
             })

    assert {:error, %{kind: :limit_exceeded, details: %{reason: :deployment_upload_busy}}} =
             Store.acquire_manifest_upload_lease(%{
               acquire
               | context: third_context,
                 lease_id: "global-third"
             })

    assert :ok =
             Store.release_manifest_upload_lease(%ReleaseManifestUploadLease{
               context: context.deployment_context,
               lease_id: "upload-one"
             })

    assert :ok =
             Store.release_manifest_upload_lease(%ReleaseManifestUploadLease{
               context: second_context,
               lease_id: "upload-two"
             })

    assert :ok = Store.acquire_manifest_upload_lease(%{acquire | lease_id: "upload-three"})
  end

  test "acceptance is atomic and permanent operation ids replay or conflict", context do
    command = accept_command(context)

    assert {:ok, :accepted, accepted} = Store.accept_manifest_deployment(command)
    assert accepted.state == :accepted
    assert accepted.service_identity == "ci-v1"

    assert %{rows: [["service:ci-v1"]]} =
             SQL.query!(
               Repo,
               "SELECT principal_id FROM favn_control.auth_platform_audit_entries WHERE action = 'manifest.deployment.accepted' AND subject_id = $1",
               [command.operation_id]
             )

    assert {:ok, :replay, replayed} = Store.accept_manifest_deployment(command)
    assert replayed.operation_id == accepted.operation_id

    assert {:error, %{kind: :conflict}} =
             Store.accept_manifest_deployment(%{
               command
               | archive_sha256: String.duplicate("c", 64)
             })

    claim = %ClaimManifestDeployment{
      platform_context: SystemContext.platform(:manifest_test, roles: [:platform_operator]),
      owner: "worker-one",
      occurred_at: DateTime.utc_now(),
      expires_at: DateTime.add(DateTime.utc_now(), 60, :second)
    }

    assert {:ok, activating} = Store.claim_manifest_deployment(claim)
    assert activating.state == :activating

    assert :ok =
             Store.update_manifest_deployment_progress(%UpdateManifestDeploymentProgress{
               platform_context: claim.platform_context,
               workspace_id: context.workspace_id,
               operation_id: command.operation_id,
               owner: claim.owner,
               fence: activating.claim_fence,
               completed: 20,
               total: 100,
               occurred_at: DateTime.utc_now()
             })

    completion = %CompleteManifestDeployment{
      platform_context: claim.platform_context,
      workspace_id: context.workspace_id,
      operation_id: command.operation_id,
      owner: claim.owner,
      fence: activating.claim_fence,
      state: :succeeded,
      deployment_id: "deployment-one",
      activation_diagnostics: ManifestActivationDiagnostics.to_map(nil),
      occurred_at: DateTime.utc_now()
    }

    assert {:error, %{kind: :conflict}} =
             Store.complete_manifest_deployment(%{completion | fence: completion.fence + 1})

    assert {:ok, succeeded} = Store.complete_manifest_deployment(completion)
    assert succeeded.state == :succeeded

    assert {:ok, persisted} =
             Store.get_manifest_deployment(%GetManifestDeployment{
               context: context.deployment_context,
               operation_id: command.operation_id
             })

    assert persisted.state == :succeeded
    assert persisted.deployment_id == "deployment-one"
    assert persisted.inspection_completed == 20
    assert persisted.inspection_total == 100
  end

  test "operation insert failure rolls back manifest publication atomically", context do
    command = accept_command(context)

    SQL.query!(Repo, """
    CREATE FUNCTION pg_temp.reject_manifest_deployment_operation()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      RAISE EXCEPTION 'injected manifest operation failure';
    END;
    $$
    """)

    SQL.query!(Repo, """
    CREATE TRIGGER reject_manifest_deployment_operation
    BEFORE INSERT ON favn_control.manifest_deployment_operations
    FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_manifest_deployment_operation()
    """)

    assert {:error, _reason} = Store.accept_manifest_deployment(command)

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.manifest_versions WHERE manifest_version_id = $1",
               [command.version.manifest_version_id]
             )

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM favn_control.manifest_deployment_operations WHERE workspace_id = $1 AND operation_id = $2",
               [context.workspace_id, command.operation_id]
             )
  end

  test "HTTP authentication and replay finish before an invalid body is read", context do
    path = "/api/orchestrator/v1/manifest-deployments/deploy-operation"
    other_workspace = provision_workspace(context, "forbidden-status")

    sentinel =
      "token=super-secret-sentinel SELECT * FROM private_table /secret/path RuntimeError"

    log =
      capture_log(fn ->
        unauthorized =
          :put
          |> conn(path, sentinel)
          |> put_req_header("authorization", "Bearer wrong-credential")
          |> put_req_header("x-favn-workspace-id", context.workspace_id)
          |> put_req_header("x-favn-archive-sha256", String.duplicate("a", 64))
          |> put_req_header("content-type", "application/gzip")
          |> put_req_header("x-request-id", "unauthorized-request")
          |> ManifestDeployment.call([])

        send(self(), {:unauthorized_response, unauthorized})
      end)

    assert_receive {:unauthorized_response, unauthorized}

    assert unauthorized.status == 401
    refute unauthorized.resp_body =~ sentinel
    refute log =~ sentinel

    forbidden =
      :get
      |> conn(path)
      |> put_req_header("authorization", "Bearer " <> context.raw_token)
      |> put_req_header("x-favn-workspace-id", other_workspace)
      |> put_req_header("x-request-id", "cross-workspace-request")
      |> ManifestDeployment.call([])

    assert forbidden.status == 403
    assert get_in(Jason.decode!(forbidden.resp_body), ["error", "code"]) == "forbidden"

    assert {:ok, :accepted, _operation} =
             context |> accept_command() |> Store.accept_manifest_deployment()

    replay =
      :put
      |> conn(path, "not a gzip archive")
      |> put_req_header("authorization", "Bearer " <> context.raw_token)
      |> put_req_header("x-favn-workspace-id", context.workspace_id)
      |> put_req_header("x-favn-archive-sha256", String.duplicate("a", 64))
      |> put_req_header("content-type", "application/gzip")
      |> put_req_header("x-request-id", "replay-request")
      |> ManifestDeployment.call([])

    assert replay.status == 200
    assert get_in(Jason.decode!(replay.resp_body), ["data", "operation", "state"]) == "accepted"
  end

  test "first-party archive deployment activates and replays after inspection", context do
    Sandbox.mode(Repo, {:shared, self()})
    operation_id = "archive-activation-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)
    archive_body = File.read!(archive_path)

    accepted = upload_archive(context, operation_id, archive_sha256, archive_body)
    assert accepted.status == 202
    assert get_in(Jason.decode!(accepted.resp_body), ["data", "operation", "state"]) == "accepted"

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})
    start_supervised!({ManifestDeploymentDispatcher, concurrency: 1})

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    assert RunnerRegistry.count(binding.runner_pool, binding.required_runner_release_id) == 0

    demand = await_runner_demand(binding, 1)
    assert demand.status == 200
    assert Jason.decode!(demand.resp_body) == %{"outstanding" => 1}

    runner_id = "manifest-inspection-runner-#{System.unique_integer([:positive])}"

    runner_agent =
      spawn_link(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(runner_agent, :stop) end)

    registration = %Registration{
      runner_instance_id: runner_id,
      boot_id: "manifest-inspection-boot",
      beam_node: Atom.to_string(node()),
      runner_pool: binding.runner_pool,
      required_runner_release_id: binding.required_runner_release_id,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, %{status: :accepted} = registration_ack} =
             RunnerRegistry.register(registration, runner_agent)

    assert {:ok, assignment} =
             RunnerTasks.claim(%ClaimRequest{
               command_id: "claim-#{runner_id}",
               issued_at: DateTime.utc_now(),
               runner_instance_id: runner_id,
               runner_session_generation: registration_ack.runner_session_generation,
               runner_pool: binding.runner_pool,
               required_runner_release_id: binding.required_runner_release_id,
               supported_task_kinds: [:relation_inspection],
               capabilities: ["relation_inspection"]
             })

    now = DateTime.utc_now()

    assert {:ok, _started} =
             RunnerTasks.started(%Started{
               workspace_id: assignment.workspace_id,
               task_id: assignment.task_id,
               runner_instance_id: runner_id,
               runner_session_generation: registration_ack.runner_session_generation,
               assignment_generation: assignment.assignment_generation,
               issued_at: now,
               occurred_at: now
             })

    assert {:ok, _ack} =
             RunnerTasks.complete(%Result{
               workspace_id: assignment.workspace_id,
               task_id: assignment.task_id,
               task_kind: assignment.task_kind,
               runner_instance_id: runner_id,
               runner_session_generation: registration_ack.runner_session_generation,
               assignment_generation: assignment.assignment_generation,
               outcome: :succeeded,
               retry_class: :terminal,
               result: %RelationInspectionResult{
                 asset_ref: asset.ref,
                 required_runner_release_id: binding.required_runner_release_id,
                 relation_ref: asset.relation,
                 relation: nil,
                 columns: [],
                 table_metadata: %{},
                 inspected_at: DateTime.utc_now()
               },
               error: nil,
               finished_at: DateTime.utc_now()
             })

    terminal = await_deployment(context, operation_id)
    assert terminal.status == 200

    assert %{
             "data" => %{
               "operation" => %{
                 "state" => "succeeded",
                 "progress" => %{
                   "inspection_completed" => 1,
                   "inspection_total" => 1
                 }
               }
             }
           } = Jason.decode!(terminal.resp_body)

    assert {:ok, active} = Manifests.active_runtime(context.workspace_context)
    assert active.manifest_version_id == context.version.manifest_version_id

    replayed = upload_archive(context, operation_id, archive_sha256, archive_body)
    assert replayed.status == 200

    assert get_in(Jason.decode!(replayed.resp_body), ["data", "operation", "state"]) ==
             "succeeded"

    conflicting =
      upload_archive(context, operation_id, String.duplicate("f", 64), "body is not read")

    assert conflicting.status == 409

    assert get_in(Jason.decode!(conflicting.resp_body), ["error", "code"]) ==
             "deployment_operation_conflict"
  end

  test "first-party archive deployment reports a runner-start timeout", context do
    Sandbox.mode(Repo, {:shared, self()})
    operation_id = "archive-timeout-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)
    archive_body = File.read!(archive_path)
    accepted = upload_archive(context, operation_id, archive_sha256, archive_body)
    assert accepted.status == 202

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})
    start_supervised!({ManifestDeploymentDispatcher, concurrency: 1, inspection_timeout_ms: 50})

    terminal = await_deployment(context, operation_id)
    assert terminal.status == 200

    assert %{
             "data" => %{
               "operation" => %{
                 "state" => "needs_attention",
                 "progress" => %{
                   "inspection_completed" => 1,
                   "inspection_total" => 1
                 },
                 "activation_diagnostics" => %{
                   "unresolved_inspection_count" => 1,
                   "unresolved_inspections" => [
                     %{"reason_code" => "physical_inspection_runner_start_timeout"}
                   ]
                 }
               }
             }
           } = Jason.decode!(terminal.resp_body)

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    assert await_runner_demand(binding, 0).status == 200

    replayed = upload_archive(context, operation_id, archive_sha256, archive_body)
    assert replayed.status == 200

    assert get_in(Jason.decode!(replayed.resp_body), ["data", "operation", "state"]) ==
             "needs_attention"
  end

  test "reclaim after the durable deadline cancels existing queued inspection demand", context do
    Sandbox.mode(Repo, {:shared, self()})
    operation_id = "expired-reclaim-#{System.unique_integer([:positive])}"

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    deadline_at = DateTime.add(DateTime.utc_now(), 50, :millisecond)

    request = %RelationInspectionRequest{
      manifest_version_id: context.version.manifest_version_id,
      manifest_content_hash: context.version.content_hash,
      required_runner_release_id: binding.required_runner_release_id,
      asset_ref: asset.ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    assert {:ok, %{status: :queued}} =
             OperationRunnerTasks.ensure(
               context.workspace_context,
               context.version,
               asset.ref,
               :relation_inspection,
               request,
               {:deployment_target_inspection, operation_id, asset.target_descriptor.target_id},
               deadline_at: deadline_at
             )

    assert Jason.decode!(await_runner_demand(binding, 1).resp_body) == %{"outstanding" => 1}
    Process.sleep(60)

    selection = %DeploymentPlanner{
      common_assets: [asset.ref],
      common_pipelines: [],
      workspace_assets: [],
      workspace_pipelines: []
    }

    assert {:ok, [decision]} =
             TargetCompatibilityPlanner.plan(
               context.platform_context,
               context.workspace_context,
               context.version,
               selection,
               operation_id: operation_id,
               inspection_deadline_at: deadline_at
             )

    assert decision.reason_code == "physical_inspection_runner_start_timeout"
    assert Jason.decode!(await_runner_demand(binding, 0).resp_body) == %{"outstanding" => 0}
  end

  test "an active inspection timeout does not freeze a decision before cancellation is terminal",
       context do
    Sandbox.mode(Repo, {:shared, self()})
    context = with_sibling_asset(context)
    operation_id = "archive-active-timeout-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)
    archive_body = File.read!(archive_path)
    assert upload_archive(context, operation_id, archive_sha256, archive_body).status == 202

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})

    start_supervised!(
      {ManifestDeploymentDispatcher, concurrency: 1, inspection_timeout_ms: 1_000}
    )

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    assert await_runner_demand(binding, 2).status == 200

    runner_id = "manifest-stalled-runner-#{System.unique_integer([:positive])}"

    runner_agent =
      spawn_link(fn ->
        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> send(runner_agent, :stop) end)

    registration = %Registration{
      runner_instance_id: runner_id,
      boot_id: "manifest-stalled-boot",
      beam_node: Atom.to_string(node()),
      runner_pool: binding.runner_pool,
      required_runner_release_id: binding.required_runner_release_id,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"]
    }

    assert {:ok, %{status: :accepted} = registration_ack} =
             RunnerRegistry.register(registration, runner_agent)

    assert {:ok, assignment} =
             RunnerTasks.claim(%ClaimRequest{
               command_id: "claim-#{runner_id}",
               issued_at: DateTime.utc_now(),
               runner_instance_id: runner_id,
               runner_session_generation: registration_ack.runner_session_generation,
               runner_pool: binding.runner_pool,
               required_runner_release_id: binding.required_runner_release_id,
               supported_task_kinds: [:relation_inspection],
               capabilities: ["relation_inspection"]
             })

    now = DateTime.utc_now()

    assert {:ok, _started} =
             RunnerTasks.started(%Started{
               workspace_id: assignment.workspace_id,
               task_id: assignment.task_id,
               runner_instance_id: runner_id,
               runner_session_generation: registration_ack.runner_session_generation,
               assignment_generation: assignment.assignment_generation,
               issued_at: now,
               occurred_at: now
             })

    terminal = await_deployment(context, operation_id)
    assert terminal.status == 200

    assert %{
             "data" => %{
               "operation" => %{
                 "state" => "failed",
                 "failure_class" => "inspection_timeout_reconciliation_failed"
               }
             }
           } = Jason.decode!(terminal.resp_body)

    assert {:ok, %{status: :cancelling}} =
             OperationRunnerTasks.fetch(context.workspace_context, assignment.task_id)

    assert %{rows: [["cancelled"], ["cancelling"]]} =
             SQL.query!(
               Repo,
               """
               SELECT status
               FROM favn_control.runner_tasks
               WHERE workspace_id = $1 AND task_kind = 'relation_inspection'
               ORDER BY status
               """,
               [context.workspace_id]
             )

    assert Jason.decode!(await_runner_demand(binding, 1).resp_body) == %{"outstanding" => 1}

    assert {:error, _reason} = Manifests.active_runtime(context.workspace_context)
  end

  test "an expired worker claim is recovered under a new fence", context do
    assert {:ok, :accepted, _operation} =
             context |> accept_command() |> Store.accept_manifest_deployment()

    claimed_at = DateTime.utc_now()

    first_claim = %ClaimManifestDeployment{
      platform_context: SystemContext.platform(:manifest_test, roles: [:platform_operator]),
      owner: "worker-one",
      occurred_at: claimed_at,
      expires_at: DateTime.add(claimed_at, 1, :second)
    }

    assert {:ok, first} = Store.claim_manifest_deployment(first_claim)
    assert first.claim_fence == 1

    recovered_at = DateTime.add(claimed_at, 2, :second)

    assert {:ok, recovered} =
             Store.claim_manifest_deployment(%{
               first_claim
               | owner: "worker-two",
                 occurred_at: recovered_at,
                 expires_at: DateTime.add(recovered_at, 60, :second)
             })

    assert recovered.operation_id == first.operation_id
    assert recovered.claim_fence == 2
    assert recovered.activating_at == first.activating_at

    assert ManifestDeploymentDispatcher.inspection_deadline_at(recovered, 300_000) ==
             ManifestDeploymentDispatcher.inspection_deadline_at(first, 300_000)

    assert {:error, %{kind: :conflict}} =
             Store.update_manifest_deployment_progress(%UpdateManifestDeploymentProgress{
               platform_context: first_claim.platform_context,
               workspace_id: context.workspace_id,
               operation_id: first.operation_id,
               owner: first_claim.owner,
               fence: first.claim_fence,
               completed: 1,
               total: 1,
               occurred_at: recovered_at
             })
  end

  test "a second worker retries after the crashed worker reservation becomes stale", context do
    assert {:ok, :accepted, _operation} =
             context |> accept_command() |> Store.accept_manifest_deployment()

    claimed_at = DateTime.utc_now()

    claim = %ClaimManifestDeployment{
      platform_context: SystemContext.platform(:manifest_test, roles: [:platform_operator]),
      owner: "worker-one",
      occurred_at: claimed_at,
      expires_at: DateTime.add(claimed_at, 1, :second)
    }

    assert {:ok, first} = Store.claim_manifest_deployment(claim)
    idempotency = activation_idempotency(first)

    assert {:ok, {:new, first_reservation}} =
             Store.begin_manifest_deployment(%BeginManifestDeployment{
               workspace_context: context.workspace_context,
               idempotency: idempotency
             })

    assert first_reservation.reservation_generation == 1

    recovered_at = DateTime.add(claimed_at, 2, :second)

    assert {:ok, recovered} =
             Store.claim_manifest_deployment(%{
               claim
               | owner: "worker-two",
                 occurred_at: recovered_at,
                 expires_at: DateTime.add(recovered_at, 45, :second)
             })

    assert {:error, in_progress} =
             Store.begin_manifest_deployment(%BeginManifestDeployment{
               workspace_context: context.workspace_context,
               idempotency: idempotency
             })

    assert in_progress.kind == :conflict
    assert in_progress.details.reason == :command_in_progress

    assert :ok =
             ManifestDeploymentDispatcher.complete_activation(
               recovered,
               "worker-two",
               {:error, in_progress}
             )

    SQL.query!(
      Repo,
      """
      UPDATE favn_control.idempotency_records
      SET updated_at = clock_timestamp() - interval '61 seconds'
      WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
        AND principal_id = $4 AND key_hash = $5
      """,
      [
        context.workspace_id,
        idempotency.operation,
        Atom.to_string(idempotency.principal_kind),
        idempotency.principal_id,
        idempotency.key_hash
      ]
    )

    retried_at = DateTime.add(recovered_at, 1, :second)

    assert {:ok, retried} =
             Store.claim_manifest_deployment(%{
               claim
               | owner: "worker-two",
                 occurred_at: retried_at,
                 expires_at: DateTime.add(retried_at, 45, :second)
             })

    assert retried.claim_fence == recovered.claim_fence + 1

    assert {:ok, {:new, second_reservation}} =
             Store.begin_manifest_deployment(%BeginManifestDeployment{
               workspace_context: context.workspace_context,
               idempotency: idempotency
             })

    assert second_reservation.reservation_generation == 2
  end

  test "acceptance rejects an expired lease after upload admission is taken over", context do
    command = accept_command(context)

    SQL.query!(
      Repo,
      "UPDATE favn_control.manifest_deployment_upload_leases SET expires_at = $1 WHERE lease_id = $2",
      [DateTime.add(command.occurred_at, -1, :second), command.upload_lease_id]
    )

    takeover_at = DateTime.add(command.occurred_at, 1, :second)
    takeover_lease_id = "takeover-#{System.unique_integer([:positive])}"

    assert :ok =
             Store.acquire_manifest_upload_lease(%AcquireManifestUploadLease{
               context: context.deployment_context,
               lease_id: takeover_lease_id,
               occurred_at: takeover_at,
               expires_at: DateTime.add(takeover_at, 60, :second)
             })

    assert {:error, %{kind: :conflict, details: %{reason: :manifest_upload_lease_lost}}} =
             Store.accept_manifest_deployment(command)

    assert {:ok, :accepted, _operation} =
             Store.accept_manifest_deployment(%{
               command
               | upload_lease_id: takeover_lease_id,
                 occurred_at: takeover_at
             })
  end

  test "workspace activation leases reject overlap and fence takeover", context do
    now = DateTime.utc_now()

    acquire = %AcquireManifestActivationLease{
      workspace_context: context.workspace_context,
      operation_id: "operation-one",
      owner: "owner-one",
      occurred_at: now,
      expires_at: DateTime.add(now, 45, :second)
    }

    assert {:ok, 1} = Store.acquire_manifest_activation_lease(acquire)

    assert {:error, %{kind: :conflict, details: %{reason: :manifest_activation_in_progress}}} =
             Store.acquire_manifest_activation_lease(%{
               acquire
               | operation_id: "operation-two",
                 owner: "owner-two"
             })

    takeover_at = DateTime.add(now, 46, :second)

    assert {:ok, 2} =
             Store.acquire_manifest_activation_lease(%{
               acquire
               | operation_id: "operation-two",
                 owner: "owner-two",
                 occurred_at: takeover_at,
                 expires_at: DateTime.add(takeover_at, 45, :second)
             })

    assert {:error, %{kind: :conflict}} =
             Store.release_manifest_activation_lease(%ReleaseManifestActivationLease{
               workspace_context: context.workspace_context,
               operation_id: "operation-one",
               owner: "owner-one",
               fence: 1
             })
  end

  defp accept_command(context) do
    occurred_at = DateTime.utc_now()
    lease_id = "accept-#{System.unique_integer([:positive])}"

    assert :ok =
             Store.acquire_manifest_upload_lease(%AcquireManifestUploadLease{
               context: context.deployment_context,
               lease_id: lease_id,
               occurred_at: occurred_at,
               expires_at: DateTime.add(occurred_at, 60, :second)
             })

    %AcceptManifestDeployment{
      context: context.deployment_context,
      platform_context: context.platform_context,
      workspace_context: context.workspace_context,
      operation_id: "deploy-operation",
      upload_lease_id: lease_id,
      archive_sha256: String.duplicate("a", 64),
      request_fingerprint: String.duplicate("b", 64),
      version: context.version,
      occurred_at: occurred_at
    }
  end

  defp build_archive(context) do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_manifest_deployment_#{System.unique_integer([:positive])}"
      )

    bundle_dir = Path.join(root, "bundle")
    archive_path = Path.join(root, "manifest.tar.gz")
    on_exit(fn -> File.rm_rf(root) end)

    packages = Map.get(context, :packages, [context.package])
    assert {:ok, publication} = Publication.from_parts(context.version, packages)
    assert :ok = ManifestBuilder.write_bundle(bundle_dir, publication)
    assert {:ok, archive} = ManifestArchive.write(bundle_dir, archive_path)
    {archive_path, archive.sha256}
  end

  defp upload_archive(context, operation_id, archive_sha256, body) do
    :put
    |> conn("/api/orchestrator/v1/manifest-deployments/#{operation_id}", body)
    |> put_req_header("authorization", "Bearer " <> context.raw_token)
    |> put_req_header("x-favn-workspace-id", context.workspace_id)
    |> put_req_header("x-favn-archive-sha256", archive_sha256)
    |> put_req_header("content-type", "application/gzip")
    |> put_req_header("x-request-id", "request-#{operation_id}")
    |> ManifestDeployment.call([])
  end

  defp start_runner_control_plane do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    {:ok, capacity_token} =
      ServiceTokens.from_raw_token(
        "manifest-capacity-scaler",
        [:capacity_reader],
        @capacity_token,
        "FAVN_ORCHESTRATOR_CAPACITY_READER_TOKEN"
      )

    Application.put_env(:favn_orchestrator, :api_service_tokens, [capacity_token])
    on_exit(fn -> restore_env(:api_service_tokens, previous_tokens) end)

    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerTaskWaitSupervisor})
    start_supervised!({RunnerTaskResultRouter, []})
    start_supervised!({RunnerRegistry, []})
    start_supervised!({RunnerQueueSupervisor, []})
    start_supervised!({RunnerDemandLimiter, []})
  end

  defp await_runner_demand(binding, expected, remaining \\ 300)

  defp await_runner_demand(_binding, expected, 0),
    do: flunk("runner demand did not reach #{expected}")

  defp await_runner_demand(binding, expected, remaining) do
    response =
      :get
      |> conn(
        "/internal/runner-demand/#{binding.runner_pool}/#{binding.required_runner_release_id}"
      )
      |> put_req_header("authorization", "Bearer " <> @capacity_token)
      |> Router.call(Router.init([]))

    if response.status == 200 and
         get_in(Jason.decode!(response.resp_body), ["outstanding"]) == expected do
      response
    else
      Process.sleep(10)
      await_runner_demand(binding, expected, remaining - 1)
    end
  end

  defp await_deployment(context, operation_id, remaining \\ 300)

  defp await_deployment(_context, operation_id, 0),
    do: flunk("manifest deployment #{operation_id} did not reach a terminal state")

  defp await_deployment(context, operation_id, remaining) do
    response =
      :get
      |> conn("/api/orchestrator/v1/manifest-deployments/#{operation_id}")
      |> put_req_header("authorization", "Bearer " <> context.raw_token)
      |> put_req_header("x-favn-workspace-id", context.workspace_id)
      |> put_req_header("x-request-id", "status-#{operation_id}")
      |> ManifestDeployment.call([])

    state = get_in(Jason.decode!(response.resp_body), ["data", "operation", "state"])

    if state in ["succeeded", "needs_attention", "failed", "unknown"] do
      response
    else
      Process.sleep(10)
      await_deployment(context, operation_id, remaining - 1)
    end
  end

  defp activation_idempotency(operation) do
    {:ok, fingerprint} = Base.decode16(operation.request_fingerprint, case: :lower)

    {:ok, idempotency} =
      CommandIdempotency.new(
        "manifest.activate",
        :service,
        "manifest-deployment:" <> operation.service_identity,
        :crypto.hash(:sha256, operation.operation_id),
        fingerprint,
        DateTime.add(DateTime.utc_now(), 365, :day)
      )

    idempotency
  end

  defp execution_package(ref) do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/storage_v2/manifest_deployments_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
    package
  end

  defp with_sibling_asset(context) do
    ref = {MyApp.ManifestDeploymentSiblingAsset, :asset}
    package = execution_package(ref)

    asset =
      FavnTestSupport.with_target_descriptor(%Asset{
        ref: ref,
        module: elem(ref, 0),
        name: elem(ref, 1),
        type: :sql,
        relation: RelationRef.new!(connection: :warehouse, schema: "manifest", name: "sibling"),
        materialization: :table,
        execution_package_hash: package.content_hash
      })

    manifest =
      %Manifest{assets: context.version.manifest.assets ++ [asset]}
      |> FavnTestSupport.with_manifest_graph()
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} = Version.new(manifest)

    :ok = ExecutionPackages.register(context.platform_context, [package])

    context
    |> Map.put(:version, version)
    |> Map.put(:packages, [context.package, package])
  end

  defp provision_workspace(context, suffix) do
    workspace_id = "#{context.workspace_id}-#{suffix}"

    :ok =
      Store.provision_workspace(%ProvisionWorkspace{
        platform_context: context.platform_context,
        workspace_id: workspace_id,
        slug: workspace_id,
        display_name: "Manifest upload #{suffix}",
        occurred_at: DateTime.utc_now()
      })

    workspace_id
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
