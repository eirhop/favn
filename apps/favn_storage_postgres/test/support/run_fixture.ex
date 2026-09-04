defmodule FavnStoragePostgres.TestSupport.RunFixture do
  @moduledoc false
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Commands, as: C
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus
  alias FavnStoragePostgres.Registry.Store, as: RegistryStore
  alias FavnStoragePostgres.Runs.Store, as: RunStore

  def create(workspace_id, run_ids) do
    {:ok, platform} =
      FavnOrchestrator.Persistence.PlatformContext.new("run-fixture", "run-fixture", [
        :platform_admin
      ])

    {:ok, context} =
      FavnOrchestrator.Persistence.WorkspaceContext.new(workspace_id, "run-fixture", [
        :workspace_admin
      ])

    now = DateTime.utc_now()

    manifest = %Manifest{
      metadata: %{"fixture" => workspace_id},
      assets: [%Favn.Manifest.Asset{ref: {__MODULE__, :asset}, module: __MODULE__, name: :asset}],
      pipelines: []
    }

    {:ok, version} =
      Version.new(
        manifest
        |> FavnTestSupport.with_manifest_contract()
        |> FavnTestSupport.with_manifest_graph(),
        manifest_version_id: "mv-" <> workspace_id
      )

    {:ok, version} =
      RegistryStore.register_manifest(%C.RegisterManifest{
        platform_context: platform,
        version: version
      })

    unless FavnStoragePostgres.Repo.get_by(FavnStoragePostgres.Schemas.Workspace,
             workspace_id: workspace_id
           ) do
      slug = workspace_id |> String.downcase() |> String.replace(~r/[^a-z0-9-]/, "-")

      :ok =
        RegistryStore.provision_workspace(%C.ProvisionWorkspace{
          platform_context: platform,
          workspace_id: workspace_id,
          slug: slug,
          display_name: workspace_id,
          occurred_at: now
        })
    end

    target_id = TargetStatus.target_id_for_asset({__MODULE__, :asset})
    deployment_id = "deploy-" <> workspace_id

    {:ok, _} =
      RegistryStore.deploy_manifest(%C.DeployManifest{
        platform_context: platform,
        workspace_context: context,
        deployment_id: deployment_id,
        manifest_version_id: version.manifest_version_id,
        configuration: %{"resources" => %{}},
        targets: [
          %C.DeploymentTarget{
            target_kind: :asset,
            target_id: target_id,
            selection_source: :common,
            customer_visible: true,
            descriptor: %{"target_id" => target_id, "label" => target_id}
          }
        ],
        schedules: [],
        capacity_scopes: [],
        occurred_at: now
      })

    Enum.each(run_ids, fn id ->
      run =
        RunState.new(
          id: id,
          workspace_id: workspace_id,
          deployment_id: deployment_id,
          manifest_version_id: version.manifest_version_id,
          manifest_content_hash: version.content_hash,
          runner_releases: version.runner_releases,
          asset_ref: {__MODULE__, :asset},
          target_refs: [{__MODULE__, :asset}]
        )

      {:ok, _} =
        RunStore.create_run(%C.CreateRun{
          workspace_context: context,
          command_id: "create-" <> id,
          deployment_id: deployment_id,
          run: run,
          targets: [
            %C.RunTarget{
              target_kind: :asset,
              target_id: target_id,
              target_module: Atom.to_string(__MODULE__),
              target_name: "asset",
              is_primary: true
            }
          ],
          event: %{
            run_id: id,
            sequence: 1,
            event_type: :run_submitted,
            status: :pending,
            occurred_at: run.inserted_at
          }
        })
    end)

    context
  end
end
