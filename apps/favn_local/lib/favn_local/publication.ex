defmodule FavnLocal.Publication do
  @moduledoc false

  alias FavnLocal.ActivationObserver
  alias Favn.Manifest.Publication
  alias FavnOrchestrator.ExecutionPackages
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.ManifestDeployments
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @spec build(String.t()) :: {:ok, Publication.t()} | {:error, term()}
  def build(runner_release_id) when is_binary(runner_release_id) do
    with {:ok, build} <-
           FavnAuthoring.build_manifest_with_uniform_runner_release(runner_release_id) do
      FavnAuthoring.prepare_manifest_publication(build)
    end
  end

  @type ownership :: %{operation_id: String.t(), session_id: String.t()}

  @spec reload(Publication.t(), Publication.t(), map(), String.t(), ownership() | nil) ::
          {:ok, map()} | {:error, term()}
  def reload(publication, previous, deployment, workspace_id, ownership \\ nil) do
    with {:ok, unchanged?} <- unchanged?(publication, previous, deployment, workspace_id) do
      if unchanged? do
        {:ok,
         Map.merge(deployment, %{
           reload_status: :unchanged,
           execution_packages: %{provided: length(publication.execution_packages), registered: 0},
           phases: %{
             execution_packages_ms: 0,
             manifest_publication_ms: 0,
             manifest_activation_ms: 0,
             deployment_ms: 0
           }
         })}
      else
        with {:ok, result} <- deploy(publication, workspace_id, nil, ownership) do
          {:ok, Map.put(result, :reload_status, :manifest_deployed)}
        end
      end
    end
  end

  defp unchanged?(publication, previous, deployment, workspace_id) do
    if publication.version.content_hash == previous.version.content_hash do
      with {:ok, workspace} <-
             WorkspaceContext.new(workspace_id, "favn-local", [:platform_operator]),
           {:ok, runtime} <- Manifests.active_runtime(workspace) do
        {:ok,
         runtime.deployment_id == deployment.deployment_id and
           runtime.manifest_version_id == deployment.manifest_version_id and
           runtime.runner_releases == deployment.runner_releases}
      end
    else
      {:ok, false}
    end
  end

  @spec deploy(Publication.t(), String.t(), String.t() | nil, ownership() | nil) ::
          {:ok, map()} | {:error, term()}
  def deploy(
        %Publication{} = publication,
        workspace_id,
        maintenance_token \\ nil,
        ownership \\ nil
      )
      when is_binary(workspace_id) do
    with {:ok, platform} <-
           PlatformContext.new("favn-local", "favn-local", [:platform_admin]),
         {:ok, workspace} <-
           WorkspaceContext.new(workspace_id, "favn-local", [:platform_operator]),
         %{operation_id: operation_id} <- ownership,
         {:ok, permit} <- acquire_admission(maintenance_token) do
      try do
        with :ok <- Lifecycle.delegate_deployment(permit, workspace_id, operation_id) do
          deploy_with_permit(platform, workspace, publication, ownership)
        end
      after
        release_admission(permit)
      end
    else
      nil -> {:error, :local_deployment_owner_required}
      error -> error
    end
  end

  defp deploy_with_permit(platform, workspace, publication, ownership) do
    version = publication.version
    started_at = now_ms()

    {execution_packages_ms, package_result} =
      timed(fn -> register_missing_packages(platform, publication.execution_packages) end)

    with {:ok, package_counts} <- package_result do
      {manifest_publication_ms, publication_result} =
        timed(fn -> Manifests.publish(platform, version) end)

      with {:ok, _status, canonical} <- publication_result do
        {manifest_activation_ms, activation_result} =
          timed(fn ->
            with %{operation_id: operation_id, session_id: session_id} <- ownership,
                 {:ok, _status, _operation} <-
                   ManifestDeployments.accept_local(
                     workspace,
                     operation_id,
                     session_id,
                     canonical.manifest_version_id
                   ) do
              ActivationObserver.await(workspace, operation_id, now_ms() + 330_000)
            else
              {:error, _reason} = error -> error
            end
          end)

        deployment_result(
          activation_result,
          package_counts,
          %{
            execution_packages_ms: execution_packages_ms,
            manifest_publication_ms: manifest_publication_ms,
            manifest_activation_ms: manifest_activation_ms,
            deployment_ms: max(now_ms() - started_at, 0)
          }
        )
      end
    end
  end

  defp deployment_result({:ok, runtime}, package_counts, phases) do
    {:ok,
     %{
       manifest_version_id: runtime.manifest_version_id,
       runner_releases: runtime.runner_releases,
       deployment_id: runtime.deployment_id,
       execution_packages: package_counts,
       phases: phases
     }}
  end

  defp deployment_result({:error, %Error{kind: kind} = reason}, _package_counts, _phases)
       when kind in [:internal, :unavailable],
       do: {:error, {:reload_outcome_unknown, reason}}

  defp deployment_result({:error, _reason} = error, _package_counts, _phases), do: error

  defp register_missing_packages(platform, packages) do
    hashes = Enum.map(packages, & &1.content_hash)

    with {:ok, missing_hashes} <- ExecutionPackages.missing_hashes(platform, hashes) do
      missing = MapSet.new(missing_hashes)
      packages_to_register = Enum.filter(packages, &MapSet.member?(missing, &1.content_hash))

      with :ok <- register_packages(platform, packages_to_register) do
        {:ok, %{provided: length(packages), registered: length(packages_to_register)}}
      end
    end
  end

  defp register_packages(_platform, []), do: :ok
  defp register_packages(platform, packages), do: ExecutionPackages.register(platform, packages)

  defp timed(fun) do
    started_at = now_ms()
    result = fun.()
    {max(now_ms() - started_at, 0), result}
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp acquire_admission(nil), do: Lifecycle.acquire_admission()

  defp acquire_admission(token) when is_binary(token) do
    FavnOrchestrator.Lifecycle.acquire_maintenance_admission(token)
  end

  defp release_admission(permit), do: FavnOrchestrator.Lifecycle.release_admission(permit)
end
