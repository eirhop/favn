defmodule Favn.Dev.Publish do
  @moduledoc "Publishes one immutable manifest release as staged/inactive."

  alias Favn.Dev.Bootstrap.Single
  alias Favn.Dev.OrchestratorClient

  @type summary :: %{
          manifest_version_id: String.t(),
          required_runner_release_id: String.t(),
          status: String.t()
        }

  @spec run(keyword()) :: {:ok, summary()} | {:error, term()}
  def run(opts) when is_list(opts) do
    client = Keyword.get(opts, :client, OrchestratorClient)
    env = Keyword.get(opts, :env, System.get_env())

    with {:ok, manifest_path} <- required(opts, :manifest_path),
         {:ok, orchestrator_url} <- required(opts, :orchestrator_url),
         {:ok, service_token} <- required_env(env, "FAVN_ORCHESTRATOR_SERVICE_TOKEN"),
         {:ok, publication} <- Single.read_manifest_publication(manifest_path),
         {:ok, response} <-
           client.publish_manifest(orchestrator_url, service_token, publication, nil),
         {:ok, status} <- validate_registration(response, publication.version) do
      {:ok,
       %{
         manifest_version_id: publication.version.manifest_version_id,
         required_runner_release_id: publication.version.required_runner_release_id,
         status: status
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

    required_runner_release_id =
      get_in(response, ["data", "manifest", "required_runner_release_id"])

    if status in ["published", "already_published"] and
         manifest_version_id == version.manifest_version_id and
         canonical_manifest_version_id == version.manifest_version_id and
         required_runner_release_id == version.required_runner_release_id do
      {:ok, status}
    else
      {:error, :invalid_publication_response}
    end
  end

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
end
