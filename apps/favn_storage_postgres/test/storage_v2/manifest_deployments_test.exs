defmodule FavnStoragePostgres.StorageV2.ManifestDeploymentsTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestDeploymentContext
  alias FavnOrchestrator.ManifestActivationDiagnostics
  alias FavnOrchestrator.API.ManifestDeployment
  alias FavnOrchestrator.Auth.ManifestDeployerTokens
  alias FavnOrchestrator.Persistence.Commands.AcceptManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.ClaimManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.CompleteManifestDeployment
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspace
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestUploadLease
  alias FavnOrchestrator.Persistence.Commands.UpdateManifestDeploymentProgress
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetManifestDeployment
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.Registry.Store
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} =
      Config.repo_options(url: url, ssl_mode: :disable, pool: Sandbox, pool_size: 2)

    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
    :ok
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

    manifest =
      FavnTestSupport.with_manifest_contract(
        %{assets: [], pipelines: [], schedules: [], graph: %{}, metadata: %{}},
        %{}
      )

    {:ok, version} = Version.new(manifest)

    %{
      deployment_context: deployment_context,
      platform_context: platform_context,
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

  test "HTTP authentication and replay finish before an invalid body is read", context do
    path = "/api/orchestrator/v1/manifest-deployments/deploy-operation"

    unauthorized =
      :put
      |> conn(path, "not a gzip archive")
      |> put_req_header("authorization", "Bearer wrong-credential")
      |> put_req_header("x-favn-workspace-id", context.workspace_id)
      |> put_req_header("x-favn-archive-sha256", String.duplicate("a", 64))
      |> put_req_header("content-type", "application/gzip")
      |> put_req_header("x-request-id", "unauthorized-request")
      |> ManifestDeployment.call([])

    assert unauthorized.status == 401

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
    %AcceptManifestDeployment{
      context: context.deployment_context,
      platform_context: context.platform_context,
      workspace_context: context.workspace_context,
      operation_id: "deploy-operation",
      archive_sha256: String.duplicate("a", 64),
      request_fingerprint: String.duplicate("b", 64),
      version: context.version,
      occurred_at: DateTime.utc_now()
    }
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
