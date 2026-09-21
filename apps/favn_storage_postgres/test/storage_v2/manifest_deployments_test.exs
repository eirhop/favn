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
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.ManifestDeploymentDispatcher
  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestMemory.Slot, as: ManifestMemorySlot
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
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetManifestDeployment
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeState
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
  alias FavnStoragePostgres.RunnerTasks.Store, as: TaskStore
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations
  alias FavnTestSupport.CgroupFiles

  @capacity_token "7b0a9d3f8e2c4615a794b6d1038fce254ec1b73a860d924f11c7e5a098bd6230"

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} =
      Config.repo_options(url: url, ssl_mode: :disable, pool: Sandbox, pool_size: 2)

    start_supervised!({Repo, options})
    start_supervised!({Lifecycle, shutdown_drain_timeout_ms: 120_000})
    start_supervised!({ManifestMemorySlot, []})
    :ok = Lifecycle.mark_accepting()
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
    :ok
  end

  setup do
    sandbox_owner = Sandbox.start_owner!(Repo, shared: true, isolation: "REPEATABLE READ")
    on_exit(fn -> Sandbox.stop_owner(sandbox_owner) end)
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

    :ok =
      Store.register_execution_packages(%RegisterExecutionPackages{
        platform_context: platform_context,
        packages: [package]
      })

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

  test "runtime catalog retains native protection across removal and rejects unsupported reintroduction",
       context do
    alias FavnOrchestrator.Persistence.Commands, as: C
    alias FavnStoragePostgres.TestSupport.TaskManifest
    [asset] = context.version.manifest.assets

    descriptor =
      Favn.Manifest.TargetDescriptor.from_asset(asset,
        connection_definitions: %{
          warehouse: %{adapter: Favn.SQL.Adapter.DuckDB.ADBC, module: nil}
        },
        manifest_schema_version: 21,
        runner_contract_version: 17
      )

    {:ok, native} =
      Version.new(%{
        context.version.manifest
        | assets: [%{asset | target_descriptor: descriptor}]
      })

    fixture = Map.put(context, :now, DateTime.utc_now())
    TaskManifest.retain(fixture, native)

    {:ok, removed} =
      Version.new(
        %{
          native.manifest
          | assets: [
              %Asset{
                ref: {__MODULE__, :unrelated},
                module: __MODULE__,
                name: :unrelated,
                type: :elixir
              }
            ]
        }
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract()
      )

    TaskManifest.retain(fixture, removed)

    assert Repo.get_by!(FavnStoragePostgres.Schemas.AssetTargetBinding,
             workspace_id: context.workspace_id,
             target_id: descriptor.target_id
           ).desired_manifest_id == native.manifest_version_id

    untracked = %Asset{ref: asset.ref, module: asset.module, name: asset.name, type: :elixir}

    {:ok, reintroduced} =
      Version.new(
        %{native.manifest | assets: [untracked]}
        |> FavnTestSupport.with_manifest_graph()
        |> FavnTestSupport.with_manifest_contract()
      )

    assert {:ok, _} =
             Store.register_manifest(%C.RegisterManifest{
               platform_context: context.platform_context,
               version: reintroduced
             })

    assert {:error, %{details: %{reason_code: "runtime_catalog_unsupported_target_reuse"}}} =
             Store.deploy_manifest(%C.DeployManifest{
               platform_context: context.platform_context,
               workspace_context: context.workspace_context,
               deployment_id: "runtime-reintroduced",
               manifest_version_id: reintroduced.manifest_version_id,
               configuration: %{"resources" => %{}},
               occurred_at: fixture.now,
               targets: [
                 %C.DeploymentTarget{
                   target_kind: :asset,
                   target_id: descriptor.target_id,
                   selection_source: :common,
                   customer_visible: true,
                   descriptor: %{"target_id" => descriptor.target_id, "label" => "asset"}
                 }
               ]
             })
  end

  test "runtime catalog deployment fences retained old tasks and rejects downgrade", context do
    FavnStoragePostgres.TestSupport.RunFixture.create(context.workspace_id, [])
    alias FavnStoragePostgres.RuntimeCatalogGuard, as: Guard
    alias FavnStoragePostgres.Schemas.ManifestVersion
    current = Repo.get!(ManifestVersion, "mv-" <> context.workspace_id)

    old = %{
      current
      | manifest_version_id: current.manifest_version_id <> "-old",
        content_hash:
          :crypto.hash(:sha256, context.workspace_id <> "old") |> Base.encode16(case: :lower),
        runner_contract_version: 16,
        schema_version: 20
    }

    Repo.insert!(old)

    for kind <- [
          "asset_attempt",
          "generation_activate",
          "generation_marker_initialize",
          "generation_discard"
        ] do
      assert {:error, %{details: %{reason_code: "runtime_catalog_tracking_required"}}} =
               Repo.transaction(fn ->
                 Guard.start!(%{
                   workspace_id: context.workspace_id,
                   task_kind: kind,
                   manifest_version_id: old.manifest_version_id
                 })
               end)
    end

    assert {:error, %{details: %{reason_code: "runtime_catalog_contract_downgrade"}}} =
             Repo.transaction(fn ->
               ids = Guard.lock_deployment!(context.workspace_id, [])
               Guard.validate_deployment!(context.workspace_id, old.manifest_version_id, [], ids)
             end)

    assert {:ok, :ok} =
             Repo.transaction(fn ->
               Guard.start!(%{
                 workspace_id: context.workspace_id,
                 task_kind: "asset_attempt",
                 manifest_version_id: current.manifest_version_id
               })
             end)
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

    assert {:error, %{kind: :limit_exceeded, details: %{reason: :deployment_upload_busy}}} =
             Store.acquire_manifest_upload_lease(%{
               acquire
               | context: second_context,
                 lease_id: "global-second"
             })

    assert :ok =
             Store.release_manifest_upload_lease(%ReleaseManifestUploadLease{
               context: context.deployment_context,
               lease_id: "upload-one"
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
               context: second_context,
               lease_id: "upload-two"
             })

    assert :ok = Store.acquire_manifest_upload_lease(%{acquire | lease_id: "upload-three"})
  end

  test "acceptance is atomic and permanent operation ids replay or conflict", context do
    FavnStoragePostgres.TestSupport.RunFixture.create(context.workspace_id, [])
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
      deployment_id: "deploy-" <> context.workspace_id,
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
    assert persisted.deployment_id == completion.deployment_id
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

  test "HTTP upload refuses unsafe or busy manifest capacity before parsing the body", context do
    operation_id = "capacity-guard-#{System.unique_integer([:positive])}"

    unsafe =
      context
      |> upload_request(operation_id, "not a gzip archive")
      |> ManifestDeployment.call(
        capacity_check: fn -> {:error, :manifest_capacity_unavailable} end
      )

    assert unsafe.status == 503
    assert get_resp_header(unsafe, "retry-after") == ["5"]

    assert get_in(Jason.decode!(unsafe.resp_body), ["error", "code"]) ==
             "manifest_capacity_unavailable"

    assert {:ok, lease} = ManifestMemorySlot.acquire()

    try do
      busy =
        context
        |> upload_request(operation_id <> "-busy", "not a gzip archive")
        |> ManifestDeployment.call(capacity_check: fn -> :ok end)

      assert busy.status == 429
      assert get_resp_header(busy, "retry-after") == ["5"]
      assert get_in(Jason.decode!(busy.resp_body), ["error", "code"]) == "manifest_capacity_busy"
    after
      :ok = ManifestMemorySlot.release(lease)
    end
  end

  test "HTTP upload rejects insufficient or unknown mounted v1 memory with unmounted v2",
       context do
    for {value, index} <-
          Enum.with_index(["536870913", {:error, :enoent}, {:error, :eacces}, "invalid"]) do
      options =
        CgroupFiles.v1_with_unmounted_v2()
        |> Map.put("/sys/fs/cgroup/memory/memory.usage_in_bytes", value)
        |> CgroupFiles.options()

      operation_id = "v1-capacity-#{index}"

      response =
        context
        |> upload_request(operation_id, "not a gzip archive")
        |> ManifestDeployment.call(
          capacity_check: fn -> ManifestMemory.ensure_headroom(options) end
        )

      assert response.status == 503
      assert get_resp_header(response, "retry-after") == ["5"]

      assert get_in(Jason.decode!(response.resp_body), ["error", "code"]) ==
               "manifest_capacity_unavailable"

      assert {:error, %Error{kind: :not_found}} =
               Store.get_manifest_deployment(%GetManifestDeployment{
                 context: context.deployment_context,
                 operation_id: operation_id
               })
    end
  end

  test "first archive activates and replays with mounted v1 memory and unmounted v2", context do
    operation_id = "archive-activation-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)
    archive_body = File.read!(archive_path)
    options = CgroupFiles.options(CgroupFiles.v1_with_unmounted_v2())
    capacity_check = fn -> ManifestMemory.ensure_headroom(options) end

    assert {:error, %Error{kind: :not_found}} =
             Store.get_runtime_state(%GetRuntimeState{
               workspace_context: context.workspace_context
             })

    accepted =
      context
      |> upload_request(operation_id, archive_body, archive_sha256)
      |> ManifestDeployment.call(capacity_check: capacity_check)

    assert accepted.status == 202
    assert get_in(Jason.decode!(accepted.resp_body), ["data", "operation", "state"]) == "accepted"

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})

    version_gate = :atomics.new(1, [])
    :atomics.put(version_gate, 1, 1)

    start_supervised!(
      {ManifestDeploymentDispatcher,
       concurrency: 1,
       capacity_check: capacity_check,
       version_size_check: fn _version -> :atomics.get(version_gate, 1) == 1 end}
    )

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    assert RunnerRegistry.count(binding.runner_pool, binding.required_runner_release_id) == 0

    demand = await_runner_demand(binding, 1)
    assert demand.status == 200
    assert Jason.decode!(demand.resp_body) == %{"outstanding" => 1}

    runner_id = "manifest-inspection-runner-#{System.unique_integer([:positive])}"

    runner_agent =
      spawn(fn ->
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

    :atomics.put(version_gate, 1, 0)
    rejected_id = operation_id <> "-oversized"
    assert upload_archive(context, rejected_id, archive_sha256, archive_body).status == 202
    rejected = await_deployment(context, rejected_id).resp_body |> Jason.decode!()

    assert get_in(rejected, ["data", "operation", "failure_class"]) ==
             "manifest_memory_budget_exceeded"

    assert {:ok, preserved} = Manifests.active_runtime(context.workspace_context)
    assert preserved.deployment_id == active.deployment_id

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

    start_supervised!(
      {ManifestDeploymentDispatcher,
       concurrency: 1, inspection_timeout_ms: 250, capacity_check: fn -> :ok end}
    )

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

  test "activation capacity rejection releases its durable claim", context do
    operation_id = "archive-capacity-release-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)
    archive_body = File.read!(archive_path)
    assert upload_archive(context, operation_id, archive_sha256, archive_body).status == 202

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})
    test_pid = self()
    attempts = :atomics.new(1, [])

    capacity_check = fn ->
      case :atomics.add_get(attempts, 1, 1) do
        1 ->
          send(test_pid, {:capacity_check, 1, self()})
          receive do: (:reject -> {:error, :memory_capacity_unknown})

        attempt ->
          send(test_pid, {:capacity_check, attempt, self()})
          Process.sleep(:infinity)
      end
    end

    start_supervised!(
      {ManifestDeploymentDispatcher, concurrency: 1, capacity_check: capacity_check}
    )

    assert_receive {:capacity_check, 1, worker}
    send(worker, :reject)
    assert_receive {:capacity_check, 2, _worker}, 2_000
    claimed_twice = &(is_integer(&1.claim_fence) and &1.claim_fence >= 2)
    assert await_operation(context, operation_id, claimed_twice).state == :activating
  end

  test "activation preparation timeout releases its durable claim", context do
    operation_id = "archive-preparation-timeout-#{System.unique_integer([:positive])}"
    {archive_path, archive_sha256} = build_archive(context)

    assert upload_archive(context, operation_id, archive_sha256, File.read!(archive_path)).status ==
             202

    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})
    test_pid = self()
    attempts = :atomics.new(1, [])

    start_supervised!(
      {ManifestDeploymentDispatcher,
       concurrency: 1,
       capacity_check: fn -> :ok end,
       preparation_timeout_ms: 500,
       version_size_check: fn _version ->
         send(test_pid, {:preparation_started, :atomics.add_get(attempts, 1, 1)})
         Process.sleep(:infinity)
       end}
    )

    assert_receive {:preparation_started, 1}, 2_000
    assert_receive {:preparation_started, 2}, 3_000
    claimed_twice = &(is_integer(&1.claim_fence) and &1.claim_fence >= 2)
    assert await_operation(context, operation_id, claimed_twice).state == :activating
  end

  test "reclaim after the durable deadline cancels existing queued inspection demand", context do
    operation_id = "expired-reclaim-#{System.unique_integer([:positive])}"

    assert {:ok, :accepted, _} =
             Store.accept_manifest_deployment(%{
               accept_command(context)
               | operation_id: operation_id
             })

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
               deadline_at: deadline_at,
               platform_context: context.platform_context
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
      {ManifestDeploymentDispatcher,
       concurrency: 1, inspection_timeout_ms: 1_000, capacity_check: fn -> :ok end}
    )

    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)
    assert await_runner_demand(binding, 2).status == 200

    runner_id = "manifest-stalled-runner-#{System.unique_integer([:positive])}"

    runner_agent =
      spawn(fn ->
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

  test "local acceptance is pinned, leased, and cannot replace unsettled cancellation", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-first")
    assert {:ok, :accepted, first} = Store.accept_local_manifest_deployment(command)
    assert first.source == "local"
    assert first.archive_sha256 == nil
    assert {:ok, :replay, ^first} = Store.accept_local_manifest_deployment(command)

    assert {:error, %{details: %{reason: :deployment_operation_conflict}}} =
             Store.accept_local_manifest_deployment(%{command | session_id: "different"})

    assert {:error, %{details: %{reason: :local_deployment_pending}}} =
             Store.accept_local_manifest_deployment(%{command | operation_id: "local-next"})

    assert {:ok, %{state: :cancelling}} = cancel_owned(context, first.operation_id)

    assert {:error, %{details: %{reason: :local_deployment_pending}}} =
             Store.accept_local_manifest_deployment(%{command | operation_id: "local-next"})

    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, :accepted, _} =
             Store.accept_local_manifest_deployment(%{command | operation_id: "local-next"})
  end

  test "expiry cancels a local owner and renewal cannot revive it", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-expired")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    future = DateTime.add(command.occurred_at, 46, :second)

    assert {:error, %{details: %{reason: :local_deployment_session_expired}}} =
             Store.renew_local_manifest_deployment(
               %FavnOrchestrator.Persistence.Commands.RenewLocalManifestDeployment{
                 workspace_context: context.workspace_context,
                 operation_id: command.operation_id,
                 session_id: command.session_id,
                 occurred_at: future,
                 expires_at: DateTime.add(future, 45, :second)
               }
             )

    assert {:ok, _} = reconcile_owned(context, future)

    assert {:ok, %{state: :cancelled, cleanup_state: "settled"}} =
             FavnOrchestrator.ManifestDeployments.get_local(
               context.workspace_context,
               command.operation_id
             )
  end

  test "owned queued inspections cancel in bounded pages and close enqueue replay", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-queued")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    tasks = for n <- 1..101, do: owned_inspection(context, command.operation_id, n)

    assert {:ok, %{tasks: page, counts: %{"queued" => 101}}} =
             FavnOrchestrator.ManifestDeployments.inspections(
               context.workspace_context,
               command.operation_id
             )

    assert length(page) == 100

    assert {:ok, %{tasks: [_last]}} =
             FavnOrchestrator.ManifestDeployments.inspections(
               context.workspace_context,
               command.operation_id,
               after_task_id: List.last(page).task_id
             )

    assert {:ok, _} = cancel_owned(context, command.operation_id)

    assert {:error, %{details: %{reason: :deployment_inspection_admission_closed}}} =
             ensure_owned_inspection(context, command.operation_id, 1)

    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, %{counts: %{"cancelled" => 100, "queued" => 1}}} =
             FavnOrchestrator.ManifestDeployments.inspections(
               context.workspace_context,
               command.operation_id
             )

    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, %{counts: %{"cancelled" => 101}}} =
             FavnOrchestrator.ManifestDeployments.inspections(
               context.workspace_context,
               command.operation_id
             )

    assert {:error, %{details: %{reason: :deployment_inspection_admission_closed}}} =
             ensure_owned_inspection(context, command.operation_id, 1)

    assert {:ok, %{status: :cancelled}} =
             OperationRunnerTasks.fetch(context.workspace_context, hd(tasks).task_id)

    assert length(Enum.uniq_by(tasks, & &1.task_id)) == 101
  end

  test "closed deployment rejects completed-task admission replay but retains evidence",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-completed-replay")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    task = owned_inspection(context, command.operation_id, 1)
    assert {:ok, assigned} = TaskStore.claim(owned_claim(context))
    now = DateTime.utc_now()

    assert {:ok, _} =
             TaskStore.transition(%FavnOrchestrator.Persistence.Commands.TransitionRunnerTask{
               workspace_context: context.workspace_context,
               command_id: "completed-replay-start",
               task_id: task.task_id,
               runner_instance_id: assigned.assigned_runner_instance_id,
               runner_session_generation: assigned.assigned_runner_session_generation,
               assignment_generation: assigned.assignment_generation,
               transition: :running,
               issued_at: now,
               occurred_at: now
             })

    result = %RelationInspectionResult{
      asset_ref: task.payload.asset_ref,
      relation_ref: task.payload.relation,
      required_runner_release_id: task.required_runner_release_id,
      row_count: 1,
      inspected_at: now
    }

    assert {:ok, encoded} =
             Favn.Contracts.RunnerTask.PersistenceCodec.encode_result(
               :relation_inspection,
               :succeeded,
               result
             )

    assert {:ok, _} =
             TaskStore.complete(%FavnOrchestrator.Persistence.Commands.CompleteRunnerTask{
               workspace_context: context.workspace_context,
               command_id: "completed-replay-finish",
               task_id: task.task_id,
               runner_instance_id: assigned.assigned_runner_instance_id,
               runner_session_generation: assigned.assigned_runner_session_generation,
               assignment_generation: assigned.assignment_generation,
               result_version: 1,
               outcome: :succeeded,
               retry_class: :terminal,
               result: encoded,
               issued_at: now,
               occurred_at: now
             })

    assert {:ok, _} = cancel_owned(context, command.operation_id)

    assert {:error, %{details: %{reason: :deployment_inspection_admission_closed}}} =
             ensure_owned_inspection(context, command.operation_id, 1)

    assert {:ok, %{status: :succeeded, result: ^result}} =
             OperationRunnerTasks.fetch(context.workspace_context, task.task_id)
  end

  test "closed owner fences replayed claim and running transition but permits settlement",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-assigned")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    task = owned_inspection(context, command.operation_id, 1)
    claim = owned_claim(context)
    assert {:ok, assigned} = FavnStoragePostgres.RunnerTasks.Store.claim(claim)
    assert assigned.task_id == task.task_id
    assert {:ok, _} = cancel_owned(context, command.operation_id)
    assert {:error, %{kind: :fenced}} = FavnStoragePostgres.RunnerTasks.Store.claim(claim)

    transition = %FavnOrchestrator.Persistence.Commands.TransitionRunnerTask{
      workspace_context: context.workspace_context,
      command_id: "closed-start",
      task_id: task.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: assigned.assignment_generation,
      transition: :running,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    }

    assert {:error, _} = FavnStoragePostgres.RunnerTasks.Store.transition(transition)
    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, %{status: :cancelling}} =
             OperationRunnerTasks.fetch(context.workspace_context, task.task_id)

    assert {:ok, %{cleanup_state: "settling"}} =
             FavnOrchestrator.ManifestDeployments.get_local(
               context.workspace_context,
               command.operation_id
             )
  end

  test "local dispatcher records committed outcome before cancellation can report failure",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-commit")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.ManifestDeploymentTaskSupervisor})

    start_supervised!(
      {ManifestDeploymentDispatcher,
       inspection_timeout_ms: 100, capacity_check: fn -> {:error, :memory_capacity_unknown} end}
    )

    operation =
      await_operation(
        context,
        command.operation_id,
        &(&1.state in [:succeeded, :needs_attention])
      )

    assert %{"deployment_id" => deployment_id, "runtime_revision" => revision} =
             operation.activation_receipt

    assert is_integer(revision)
    assert {:ok, cancelled} = cancel_owned(context, command.operation_id)
    assert cancelled.activation_receipt == operation.activation_receipt
    assert cancelled.state == operation.state

    assert {:ok, %{deployment_id: ^deployment_id, revision: ^revision}} =
             Manifests.active_runtime(context.workspace_context)
  end

  test "cancellation before activation rejects a stale worker commit", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-cancel-first")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    now = DateTime.utc_now()

    claim = %ClaimManifestDeployment{
      platform_context: context.platform_context,
      owner: "worker",
      occurred_at: now,
      expires_at: DateTime.add(now, 45, :second)
    }

    assert {:ok, operation} = Store.claim_manifest_deployment(claim)
    assert {:ok, _} = cancel_owned(context, command.operation_id)

    assert {:error, _} =
             Manifests.deploy(
               context.platform_context,
               context.workspace_context,
               context.version.manifest_version_id,
               FavnOrchestrator.ManifestDeployments.fixed_selection(),
               deployment_id: operation.operation_id,
               activation_operation_id: operation.operation_id,
               deployment_claim: %{
                 operation_id: operation.operation_id,
                 owner: "worker",
                 fence: operation.claim_fence
               },
               activation_inspection_timeout_ms: 1,
               execution_pool_policy: %{approve_manifest_defaults: true}
             )

    assert {:error, _} = Manifests.active_runtime(context.workspace_context)
  end

  test "stop before local acceptance is durable and an existing cancelled operation still replays",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-stop-first")
    assert {:ok, :cancelled_before_acceptance} = cancel_owned(context, command.operation_id)

    assert {:error, %{details: %{reason: :local_deployment_cancelled_before_acceptance}}} =
             Store.accept_local_manifest_deployment(command)

    existing = %{command | operation_id: "local-existing"}
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(existing)
    assert {:ok, cancelled} = cancel_owned(context, existing.operation_id)
    assert {:ok, :replay, ^cancelled} = Store.accept_local_manifest_deployment(existing)
  end

  test "changed target bindings cannot reuse an owned inspection base", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-bindings")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)

    pin = %FavnOrchestrator.Persistence.Commands.PinDeploymentInspectionBase{
      workspace_context: context.workspace_context,
      operation_id: command.operation_id,
      binding_hash: :crypto.hash(:sha256, "bindings-v1")
    }

    assert :ok = Store.pin_deployment_inspection_base(pin)
    assert :ok = Store.pin_deployment_inspection_base(pin)

    assert {:error, %{details: %{reason: :deployment_inspection_base_changed}}} =
             Store.pin_deployment_inspection_base(%{
               pin
               | binding_hash: :crypto.hash(:sha256, "bindings-v2")
             })
  end

  test "a delayed heartbeat cannot revive an actually expired local session", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-delayed-renewal")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    now = DateTime.utc_now()

    SQL.query!(
      Repo,
      "UPDATE favn_control.manifest_deployment_operations SET local_expires_at=$1 WHERE workspace_id=$2 AND operation_id=$3",
      [DateTime.add(now, -5, :second), context.workspace_id, command.operation_id]
    )

    assert {:error, %{details: %{reason: :local_deployment_session_expired}}} =
             Store.renew_local_manifest_deployment(
               %FavnOrchestrator.Persistence.Commands.RenewLocalManifestDeployment{
                 workspace_context: context.workspace_context,
                 operation_id: command.operation_id,
                 session_id: command.session_id,
                 occurred_at: DateTime.add(now, -20, :second),
                 expires_at: DateTime.add(now, 25, :second)
               }
             )
  end

  test "unknown inspection execution blocks replacement until exact quiescence is attested",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-unknown")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    task = owned_inspection(context, command.operation_id, 1)
    assert {:ok, assigned} = FavnStoragePostgres.RunnerTasks.Store.claim(owned_claim(context))
    assert {:ok, _} = cancel_owned(context, command.operation_id)
    assert {:ok, _} = reconcile_owned(context)

    release = %FavnOrchestrator.Persistence.Commands.ReleaseRunnerTask{
      workspace_context: context.workspace_context,
      command_id: "release-unknown",
      task_id: task.task_id,
      runner_instance_id: assigned.assigned_runner_instance_id,
      runner_session_generation: assigned.assigned_runner_session_generation,
      assignment_generation: assigned.assignment_generation,
      disposition: :requeue,
      reason: nil,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, %{status: :unknown}} = FavnStoragePostgres.RunnerTasks.Store.release(release)
    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, %{cleanup_state: "unknown"}} =
             FavnOrchestrator.ManifestDeployments.get_local(
               context.workspace_context,
               command.operation_id
             )

    assert {:error, _} =
             Store.accept_local_manifest_deployment(%{command | operation_id: "replacement"})

    resolution = %FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections{
      workspace_context: context.workspace_context,
      operation_id: command.operation_id,
      task_assignments: %{task.task_id => assigned.assignment_generation},
      runner_stopped: true,
      backend_stopped: true,
      evidence_reference: "test-quiescence-receipt",
      occurred_at: DateTime.utc_now()
    }

    assert {:error, _} =
             Store.resolve_deployment_inspections(%{resolution | backend_stopped: false})

    assert {:error, _} =
             Store.resolve_deployment_inspections(%{
               resolution
               | task_assignments: %{task.task_id => 99}
             })

    assert {:ok, 1} = Store.resolve_deployment_inspections(resolution)
    assert {:ok, 1} = Store.resolve_deployment_inspections(resolution)
    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, %{state: :cancelled, cleanup_state: "settled"}} =
             FavnOrchestrator.ManifestDeployments.get_local(
               context.workspace_context,
               command.operation_id
             )

    assert {:ok, :accepted, _} =
             Store.accept_local_manifest_deployment(%{command | operation_id: "replacement"})
  end

  test "legacy backlog blocks local acceptance and requires verified assignment identities",
       context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    task = owned_inspection(context, nil, 1)
    command = local_command(context, "local-legacy")

    assert {:error, %{details: %{reason: :legacy_deployment_inspections_pending}}} =
             Store.accept_local_manifest_deployment(command)

    resolution = %FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections{
      workspace_context: context.workspace_context,
      operation_id: nil,
      task_assignments: %{task.task_id => 0},
      runner_stopped: true,
      backend_stopped: true,
      evidence_reference: "legacy-inventory-reviewed",
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, 1} = Store.resolve_deployment_inspections(resolution)
    assert {:ok, 1} = Store.resolve_deployment_inspections(resolution)
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
  end

  test "deployment-owned terminal evidence is excluded from standalone retention", context do
    start_owned_runtime()
    assert {:ok, _, _} = Manifests.publish(context.platform_context, context.version)
    command = local_command(context, "local-retention")
    assert {:ok, :accepted, _} = Store.accept_local_manifest_deployment(command)
    task = owned_inspection(context, command.operation_id, 1)
    assert {:ok, _} = cancel_owned(context, command.operation_id)
    assert {:ok, _} = reconcile_owned(context)
    policy = %{excluded_workspace_ids: []}
    cursor = %{"workspace_id" => context.workspace_id, "id" => task.task_id}

    result =
      FavnStoragePostgres.Maintenance.TaskRetention.delete!(
        policy,
        DateTime.add(DateTime.utc_now(), 86_400, :second),
        cursor
      )

    assert %{deleted_count: 0} = result

    assert {:ok, %{status: :cancelled}} =
             OperationRunnerTasks.fetch(context.workspace_context, task.task_id)
  end

  test "unsettled archive inspections block archive claims and local replacement", context do
    start_owned_runtime()
    command = accept_command(context)
    assert {:ok, :accepted, first} = Store.accept_manifest_deployment(command)
    task = owned_inspection(context, first.operation_id, 1)
    assert {:ok, assigned} = FavnStoragePostgres.RunnerTasks.Store.claim(owned_claim(context))

    SQL.query!(
      Repo,
      "UPDATE favn_control.manifest_deployment_operations SET state='failed', failure_class='test_failure', terminal_at=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2",
      [context.workspace_id, first.operation_id]
    )

    assert {:ok, :accepted, _} =
             Store.accept_manifest_deployment(%{command | operation_id: "archive-next"})

    claim = %ClaimManifestDeployment{
      platform_context: context.platform_context,
      owner: "next-worker",
      occurred_at: DateTime.utc_now(),
      expires_at: DateTime.add(DateTime.utc_now(), 45, :second)
    }

    assert {:ok, nil} = Store.claim_manifest_deployment(claim)

    assert {:error, %{details: %{reason: :local_deployment_pending}}} =
             Store.accept_local_manifest_deployment(local_command(context, "local-next"))

    assert {:ok, _} = reconcile_owned(context)

    resolution = %FavnOrchestrator.Persistence.Commands.ResolveDeploymentInspections{
      workspace_context: context.workspace_context,
      operation_id: first.operation_id,
      task_assignments: %{task.task_id => assigned.assignment_generation},
      runner_stopped: true,
      backend_stopped: true,
      evidence_reference: "verified-stopped",
      occurred_at: DateTime.utc_now()
    }

    assert {:ok, 1} = Store.resolve_deployment_inspections(resolution)
    assert {:ok, _} = reconcile_owned(context)
    assert {:ok, %{operation_id: "archive-next"}} = Store.claim_manifest_deployment(claim)
  end

  test "legacy unknown activation still blocks an archive successor after cleanup settles",
       context do
    command = accept_command(context)
    assert {:ok, :accepted, first} = Store.accept_manifest_deployment(command)

    SQL.query!(
      Repo,
      "UPDATE favn_control.manifest_deployment_operations SET state='unknown', request='{}', failure_class='legacy_unknown', terminal_at=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2",
      [context.workspace_id, first.operation_id]
    )

    assert {:ok, _} = reconcile_owned(context)

    assert {:ok, :accepted, _} =
             Store.accept_manifest_deployment(%{command | operation_id: "unknown-successor"})

    assert {:ok, nil} =
             Store.claim_manifest_deployment(%ClaimManifestDeployment{
               platform_context: context.platform_context,
               owner: "new-worker",
               occurred_at: DateTime.utc_now(),
               expires_at: DateTime.add(DateTime.utc_now(), 45, :second)
             })
  end

  defp start_owned_runtime do
    start_supervised!(
      {Runtime, %Runtime{backend: Backend, options: [], stores: Backend.stores()}}
    )

    start_runner_control_plane()
  end

  defp local_command(context, operation_id) do
    now = DateTime.utc_now()

    %FavnOrchestrator.Persistence.Commands.AcceptLocalManifestDeployment{
      workspace_context: context.workspace_context,
      operation_id: operation_id,
      session_id: "session-" <> operation_id,
      manifest_version_id: context.version.manifest_version_id,
      occurred_at: now,
      expires_at: DateTime.add(now, 45, :second)
    }
  end

  defp cancel_owned(context, operation_id) do
    Store.cancel_manifest_deployment(
      %FavnOrchestrator.Persistence.Commands.CancelManifestDeployment{
        workspace_context: context.workspace_context,
        operation_id: operation_id,
        reason: :local_stop,
        occurred_at: DateTime.utc_now()
      }
    )
  end

  defp reconcile_owned(_context, now \\ DateTime.utc_now()) do
    Store.reconcile_manifest_deployments(
      %FavnOrchestrator.Persistence.Commands.ReconcileManifestDeployments{
        platform_context: SystemContext.platform(:manifest_test, roles: [:platform_operator]),
        occurred_at: now
      }
    )
  end

  defp owned_inspection(context, operation_id, n) do
    assert {:ok, task} = ensure_owned_inspection(context, operation_id, n)
    task
  end

  defp ensure_owned_inspection(context, operation_id, n) do
    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)

    request = %RelationInspectionRequest{
      manifest_version_id: context.version.manifest_version_id,
      manifest_content_hash: context.version.content_hash,
      required_runner_release_id: binding.required_runner_release_id,
      asset_ref: asset.ref,
      include: [:relation, :columns, :table_metadata],
      sample_limit: 0
    }

    OperationRunnerTasks.ensure(
      context.workspace_context,
      context.version,
      asset.ref,
      :relation_inspection,
      request,
      {:deployment_target_inspection, operation_id, n},
      deployment_operation_id: operation_id,
      deadline_at: DateTime.add(DateTime.utc_now(), 300, :second),
      platform_context: context.platform_context
    )
  end

  defp owned_claim(context) do
    asset = hd(context.version.manifest.assets)
    {:ok, binding} = OperationRunnerTasks.binding(context.version, asset)

    %FavnOrchestrator.Persistence.Commands.ClaimRunnerTask{
      platform_context: context.platform_context,
      command_id: "claim-owned",
      runner_instance_id: "runner-owned",
      runner_session_generation: 1,
      runner_pool: binding.runner_pool,
      required_runner_release_id: binding.required_runner_release_id,
      supported_task_kinds: [:relation_inspection],
      capabilities: ["relation_inspection"],
      lease_duration_ms: 30_000,
      issued_at: DateTime.utc_now(),
      occurred_at: DateTime.utc_now()
    }
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
    context
    |> upload_request(operation_id, body, archive_sha256)
    |> ManifestDeployment.call(capacity_check: fn -> :ok end)
  end

  defp upload_request(context, operation_id, body, archive_sha256 \\ String.duplicate("a", 64)) do
    :put
    |> conn("/api/orchestrator/v1/manifest-deployments/#{operation_id}", body)
    |> put_req_header("authorization", "Bearer " <> context.raw_token)
    |> put_req_header("x-favn-workspace-id", context.workspace_id)
    |> put_req_header("x-favn-archive-sha256", archive_sha256)
    |> put_req_header("content-type", "application/gzip")
    |> put_req_header("x-request-id", "request-#{operation_id}")
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

  defp await_operation(context, operation_id, predicate, remaining \\ 300)

  defp await_operation(_context, operation_id, _predicate, 0),
    do: flunk("manifest deployment #{operation_id} did not reach the expected state")

  defp await_operation(context, operation_id, predicate, remaining) do
    {:ok, operation} =
      Store.get_manifest_deployment(%GetManifestDeployment{
        context: context.deployment_context,
        operation_id: operation_id
      })

    if predicate.(operation) do
      operation
    else
      Process.sleep(10)
      await_operation(context, operation_id, predicate, remaining - 1)
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

    :ok =
      Store.register_execution_packages(%RegisterExecutionPackages{
        platform_context: context.platform_context,
        packages: [package]
      })

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
