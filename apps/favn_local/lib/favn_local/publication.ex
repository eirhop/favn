defmodule FavnLocal.Publication do
  @moduledoc false

  alias Favn.Manifest.Publication
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @spec build(String.t()) :: {:ok, Publication.t()} | {:error, term()}
  def build(runner_release_id) when is_binary(runner_release_id) do
    with {:ok, build} <- FavnAuthoring.build_manifest(runner_release_id: runner_release_id) do
      FavnAuthoring.prepare_manifest_publication(build)
    end
  end

  @spec deploy(Publication.t(), String.t(), String.t() | nil) ::
          {:ok, map()} | {:error, term()}
  def deploy(%Publication{} = publication, workspace_id, maintenance_token \\ nil)
      when is_binary(workspace_id) do
    with {:ok, platform} <-
           PlatformContext.new("favn-local", "favn-local", [:platform_admin]),
         {:ok, workspace} <-
           WorkspaceContext.new(workspace_id, "favn-local", [:platform_operator]),
         {:ok, permit} <- acquire_admission(maintenance_token),
         result <- deploy_with_permit(platform, workspace, publication) do
      release_admission(permit)
      result
    end
  end

  defp deploy_with_permit(platform, workspace, publication) do
    version = publication.version

    with :ok <- ExecutionPackages.register(platform, publication.execution_packages),
         {:ok, _status, canonical} <- Manifests.publish(platform, version),
         {:ok, runtime} <-
           Manifests.deploy(
             platform,
             workspace,
             canonical.manifest_version_id,
             %{
               common_assets: "all",
               common_pipelines: "all",
               workspace_assets: [],
               workspace_pipelines: []
             },
             deployment_id: "deployment:local:" <> canonical.manifest_version_id,
             configuration: %{}
           ) do
      {:ok,
       %{
         manifest_version_id: runtime.manifest_version_id,
         runner_release_id: runtime.required_runner_release_id,
         deployment_id: runtime.deployment_id
       }}
    end
  end

  defp acquire_admission(nil), do: {:ok, nil}

  defp acquire_admission(token) when is_binary(token) do
    FavnOrchestrator.Lifecycle.acquire_maintenance_admission(token)
  end

  defp release_admission(nil), do: :ok
  defp release_admission(permit), do: FavnOrchestrator.Lifecycle.release_admission(permit)
end
