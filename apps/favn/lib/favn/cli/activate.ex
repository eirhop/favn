defmodule Favn.CLI.Activate do
  @moduledoc "Activates one exact staged manifest for one workspace."

  alias Favn.CLI.OrchestratorClient

  @type summary :: %{
          manifest_version_id: String.t(),
          workspace_id: String.t(),
          activated?: boolean(),
          required_runner_release_id: String.t() | nil
        }

  @spec run(keyword()) :: {:ok, summary()} | {:error, term()}
  def run(opts) when is_list(opts) do
    client = Keyword.get(opts, :client, OrchestratorClient)
    env = Keyword.get(opts, :env, System.get_env())

    with {:ok, orchestrator_url} <- required(opts, :orchestrator_url),
         {:ok, manifest_version_id} <- required(opts, :manifest_version_id),
         {:ok, workspace_id} <- required(opts, :workspace_id),
         {:ok, service_token} <- required_env(env, "FAVN_ORCHESTRATOR_SERVICE_TOKEN"),
         {:ok, response} <-
           activate_manifest(
             client,
             orchestrator_url,
             service_token,
             manifest_version_id,
             workspace_id,
             Keyword.get(opts, :maintenance_token)
           ),
         {:ok, data} <- activation_data(response, manifest_version_id) do
      {:ok,
       %{
         manifest_version_id: manifest_version_id,
         workspace_id: workspace_id,
         activated?: true,
         required_runner_release_id: Map.get(data, "required_runner_release_id")
       }}
    end
  end

  defp activation_data(%{"data" => data}, manifest_version_id),
    do: validate_activation_data(data, manifest_version_id)

  defp activation_data(data, manifest_version_id),
    do: validate_activation_data(data, manifest_version_id)

  defp validate_activation_data(
         %{
           "activated" => true,
           "manifest_version_id" => manifest_version_id,
           "deployment_id" => deployment_id,
           "required_runner_release_id" => runner_release_id
         } = data,
         manifest_version_id
       )
       when is_binary(deployment_id) and deployment_id != "" do
    case Favn.Manifest.Compatibility.validate_required_runner_release_id(runner_release_id) do
      :ok -> {:ok, data}
      {:error, _reason} -> {:error, :invalid_activation_response}
    end
  end

  defp validate_activation_data(_data, _manifest_version_id),
    do: {:error, :invalid_activation_response}

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

  defp activate_manifest(
         client,
         orchestrator_url,
         service_token,
         manifest_version_id,
         workspace_id,
         maintenance_token
       )
       when is_binary(maintenance_token) and maintenance_token != "" do
    client.activate_manifest_service(
      orchestrator_url,
      service_token,
      manifest_version_id,
      workspace_id,
      maintenance_token: maintenance_token
    )
  end

  defp activate_manifest(
         client,
         orchestrator_url,
         service_token,
         manifest_version_id,
         workspace_id,
         _maintenance_token
       ) do
    client.activate_manifest_service(
      orchestrator_url,
      service_token,
      manifest_version_id,
      workspace_id
    )
  end
end
