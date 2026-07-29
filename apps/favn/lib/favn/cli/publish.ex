defmodule Favn.CLI.Publish do
  @moduledoc "Publishes one immutable manifest release as staged/inactive."

  alias Favn.CLI.OrchestratorClient
  alias FavnAuthoring.Deployment.ManifestPublication

  @type summary :: %{
          manifest_version_id: String.t(),
          runner_releases: Favn.RunnerPool.releases(),
          status: String.t()
        }

  @spec run(keyword()) :: {:ok, summary()} | {:error, term()}
  def run(opts) when is_list(opts) do
    client = Keyword.get(opts, :client, OrchestratorClient)
    env = Keyword.get(opts, :env, System.get_env())

    with {:ok, manifest_path} <- required(opts, :manifest_path),
         {:ok, orchestrator_url} <- required(opts, :orchestrator_url),
         {:ok, service_token} <- required_env(env, "FAVN_ORCHESTRATOR_SERVICE_TOKEN"),
         {:ok, publication} <- ManifestPublication.read(manifest_path),
         {:ok, response} <-
           client.publish_manifest(
             orchestrator_url,
             service_token,
             publication,
             maintenance_context(opts)
           ),
         {:ok, registration} <- validate_registration(response, publication.version) do
      {:ok,
       %{
         manifest_version_id: registration.manifest_version_id,
         runner_releases: publication.version.runner_releases,
         status: registration.status
       }}
    end
  end

  defp validate_registration(response, version) do
    status =
      get_in(response, ["data", "registration", "status"]) ||
        get_in(response, ["data", "manifest", "registration", "status"])

    manifest_version_id = get_in(response, ["data", "registration", "manifest_version_id"])

    canonical_manifest_version_id =
      get_in(response, ["data", "registration", "canonical_manifest_version_id"])

    runner_releases = get_in(response, ["data", "manifest", "runner_releases"])

    if valid_registration?(
         status,
         manifest_version_id,
         canonical_manifest_version_id,
         runner_releases,
         version
       ) do
      {:ok, %{status: status, manifest_version_id: canonical_manifest_version_id}}
    else
      {:error,
       {:invalid_publication_response,
        %{
          status: status,
          manifest_version_id: manifest_version_id,
          canonical_manifest_version_id: canonical_manifest_version_id,
          runner_releases: runner_releases
        }}}
    end
  end

  defp valid_registration?(
         "published",
         manifest_version_id,
         canonical_manifest_version_id,
         runner_releases,
         version
       ) do
    manifest_version_id == version.manifest_version_id and
      canonical_manifest_version_id == version.manifest_version_id and
      runner_releases == version.runner_releases
  end

  defp valid_registration?(
         "already_published",
         manifest_version_id,
         canonical_manifest_version_id,
         runner_releases,
         version
       ) do
    manifest_version_id == version.manifest_version_id and
      is_binary(canonical_manifest_version_id) and canonical_manifest_version_id != "" and
      runner_releases == version.runner_releases
  end

  defp valid_registration?(
         _status,
         _manifest_version_id,
         _canonical_manifest_version_id,
         _runner_releases,
         _version
       ),
       do: false

  defp required(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_option, key}}
    end
  end

  defp required_env(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required_env, name}}
    end
  end

  defp maintenance_context(opts) do
    case Keyword.get(opts, :maintenance_token) do
      token when is_binary(token) and token != "" -> %{"maintenance_token" => token}
      _missing -> nil
    end
  end
end
