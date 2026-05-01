defmodule Favn.Dev.Bootstrap.Single do
  @moduledoc """
  API-driven bootstrap workflow for the first SQLite single-node production shape.

  The workflow does not write storage directly. It verifies orchestrator service
  credentials, validates a manifest JSON file as a `Favn.Manifest.Version`, then
  registers and activates that manifest before asking the orchestrator to
  register the persisted manifest with the local runner.
  """

  alias Favn.Dev.OrchestratorClient
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Version

  @default_runner_id "single-node-local"

  @type opts :: [
          manifest_path: Path.t(),
          orchestrator_url: String.t(),
          service_token: String.t(),
          runner_id: String.t(),
          activate?: boolean(),
          client: module()
        ]

  @type summary :: %{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          runner_id: String.t(),
          activated?: boolean(),
          active_manifest_verified?: boolean(),
          active_manifest_verification: :matched | term()
        }

  @spec run(opts()) :: {:ok, summary()} | {:error, term()}
  def run(opts) when is_list(opts) do
    client = Keyword.get(opts, :client, OrchestratorClient)
    activate? = Keyword.get(opts, :activate?, true)

    with {:ok, manifest_path} <- required_string(opts, :manifest_path),
         {:ok, orchestrator_url} <- required_string(opts, :orchestrator_url),
         {:ok, service_token} <- required_string(opts, :service_token),
         runner_id <- Keyword.get(opts, :runner_id, @default_runner_id),
         :ok <- validate_runner_id(runner_id),
         :ok <- client.verify_service_token(orchestrator_url, service_token),
         {:ok, version} <- read_manifest_version(manifest_path),
         {:ok, _registration} <-
           client.publish_manifest(orchestrator_url, service_token, manifest_payload(version)),
         {:ok, activated?} <-
           maybe_activate(client, orchestrator_url, service_token, version, activate?),
         {:ok, _runner} <-
           client.register_runner(
             orchestrator_url,
             service_token,
             runner_payload(version, runner_id)
           ),
         {verified?, verification} <-
           verify_active_manifest(client, orchestrator_url, service_token, version) do
      {:ok,
       %{
         manifest_version_id: version.manifest_version_id,
         content_hash: version.content_hash,
         runner_id: runner_id,
         activated?: activated?,
         active_manifest_verified?: verified?,
         active_manifest_verification: verification
       }}
    end
  end

  @spec read_manifest_version(Path.t()) :: {:ok, Version.t()} | {:error, term()}
  def read_manifest_version(path) when is_binary(path) do
    with {:ok, bytes} <- File.read(path),
         {:ok, decoded} <- JSON.decode(bytes),
         {:ok, version} <- decode_manifest_version(decoded) do
      Version.verify(version)
    end
  end

  defp decode_manifest_version(%{"manifest" => manifest} = envelope) when is_map(manifest) do
    Version.from_published(manifest,
      manifest_version_id: Map.get(envelope, "manifest_version_id"),
      content_hash: Map.get(envelope, "content_hash"),
      schema_version: Map.get(envelope, "schema_version"),
      runner_contract_version: Map.get(envelope, "runner_contract_version"),
      serialization_format: Map.get(envelope, "serialization_format", "json-v1")
    )
  end

  defp decode_manifest_version(%{} = manifest) do
    with {:ok, content_hash} <- Identity.hash_manifest(manifest) do
      Version.new(manifest, manifest_version_id: stable_manifest_version_id(content_hash))
    end
  end

  defp decode_manifest_version(_other), do: {:error, :invalid_manifest_json}

  defp stable_manifest_version_id(content_hash) when is_binary(content_hash) do
    "mv_" <> String.slice(content_hash, 0, 32)
  end

  defp manifest_payload(%Version{} = version) do
    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      serialization_format: version.serialization_format,
      manifest: version.manifest
    }
  end

  defp runner_payload(%Version{} = version, runner_id) do
    %{
      runner_id: runner_id,
      mode: "single_node_local",
      manifest_version_id: version.manifest_version_id,
      runner_contract_version: version.runner_contract_version
    }
  end

  defp maybe_activate(_client, _url, _token, _version, false), do: {:ok, false}

  defp maybe_activate(client, orchestrator_url, service_token, version, true) do
    case client.activate_manifest(orchestrator_url, service_token, version.manifest_version_id) do
      {:ok, _result} -> {:ok, true}
      {:error, _reason} = error -> error
    end
  end

  defp verify_active_manifest(client, orchestrator_url, service_token, version) do
    case client.active_manifest(orchestrator_url, service_token, %{}) do
      {:ok, %{"manifest" => %{"manifest_version_id" => id}}}
      when id == version.manifest_version_id ->
        {true, :matched}

      {:ok, %{"manifest_version_id" => id}} when id == version.manifest_version_id ->
        {true, :matched}

      {:ok, other} ->
        {false, {:mismatch, other}}

      {:error, reason} ->
        {false, {:skipped, reason}}
    end
  end

  defp required_string(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:missing_required_option, key, value}}
    end
  end

  defp validate_runner_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_runner_id(value), do: {:error, {:invalid_runner_id, value}}
end
