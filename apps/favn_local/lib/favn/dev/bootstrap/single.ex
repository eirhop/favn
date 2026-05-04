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
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  @type opts :: [
          manifest_path: Path.t(),
          orchestrator_url: String.t(),
          service_token: String.t(),
          activate?: boolean(),
          client: module()
        ]

  @type summary :: %{
           manifest_version_id: String.t(),
           content_hash: String.t(),
           manifest_registration: String.t(),
           runner_registration: String.t(),
           activated?: boolean(),
           active_manifest_verification: :matched | {:mismatch, term()} | {:skipped, term()}
         }

  @spec run(opts()) :: {:ok, summary()} | {:error, term()}
  def run(opts) when is_list(opts) do
    client = Keyword.get(opts, :client, OrchestratorClient)
    activate? = Keyword.get(opts, :activate?, true)

    with {:ok, manifest_path} <- required_string(opts, :manifest_path),
         {:ok, orchestrator_url} <- required_string(opts, :orchestrator_url),
         {:ok, service_token} <- required_string(opts, :service_token),
         :ok <- client.verify_service_token(orchestrator_url, service_token),
         {:ok, version} <- read_manifest_version(manifest_path),
         {:ok, registration} <-
           client.publish_manifest(orchestrator_url, service_token, manifest_payload(version)),
         manifest_registration <- registration_status(registration, "manifest"),
         {:ok, activated?} <-
           maybe_activate(client, orchestrator_url, service_token, version, activate?),
         {:ok, runner} <-
           client.register_runner(
             orchestrator_url,
             service_token,
             runner_payload(version)
           ),
         runner_registration <- registration_status(runner, "registration"),
         verification <- verify_active_manifest(client, orchestrator_url, service_token, version) do
      {:ok,
       %{
         manifest_version_id: version.manifest_version_id,
         content_hash: version.content_hash,
         manifest_registration: manifest_registration,
         runner_registration: runner_registration,
         activated?: activated?,
         active_manifest_verification: verification
       }}
    end
  end

  @spec read_manifest_version(Path.t()) :: {:ok, Version.t()} | {:error, term()}
  def read_manifest_version(path) when is_binary(path) do
    with {:ok, bytes} <- read_manifest_file(path),
         metadata_opts <- packaged_manifest_opts(path),
         {:ok, version} <- decode_manifest_version_bytes(bytes, metadata_opts) do
      Version.verify(version)
    end
  end

  defp decode_manifest_version_bytes(bytes, metadata_opts) when is_binary(bytes) do
    case Serializer.decode_manifest(bytes) do
      {:ok, manifest} when metadata_opts != [] ->
        Version.from_published(manifest, metadata_opts)

      {:ok, manifest} ->
        decode_manifest_version(manifest)

      {:error, _reason} ->
        with {:ok, decoded} <- JSON.decode(bytes) do
          decode_manifest_version(decoded, metadata_opts)
        end
    end
  end

  defp packaged_manifest_opts(path) when is_binary(path) do
    path
    |> Path.dirname()
    |> Path.join("metadata.json")
    |> read_manifest_metadata_opts()
  end

  defp read_manifest_metadata_opts(metadata_path) do
    with {:ok, bytes} <- File.read(metadata_path),
         {:ok, %{"manifest" => %{} = manifest_metadata}} <- JSON.decode(bytes) do
      []
      |> put_metadata_opt(:manifest_version_id, Map.get(manifest_metadata, "manifest_version_id"))
      |> put_metadata_opt(:content_hash, Map.get(manifest_metadata, "content_hash"))
    else
      _other -> []
    end
  end

  defp put_metadata_opt(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_metadata_opt(opts, _key, _value), do: opts

  defp read_manifest_file(path) when is_binary(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:manifest_read_failed, path, reason}}
    end
  end

  defp decode_manifest_version(decoded, metadata_opts \\ [])

  defp decode_manifest_version(%{"manifest" => manifest} = envelope, metadata_opts)
       when is_map(manifest) do
    Version.from_published(manifest,
      manifest_version_id:
        Keyword.get(metadata_opts, :manifest_version_id) || Map.get(envelope, "manifest_version_id"),
      content_hash: Keyword.get(metadata_opts, :content_hash) || Map.get(envelope, "content_hash"),
      schema_version: Map.get(envelope, "schema_version"),
      runner_contract_version: Map.get(envelope, "runner_contract_version"),
      serialization_format: Map.get(envelope, "serialization_format", "json-v1")
    )
  end

  defp decode_manifest_version(%{} = manifest, metadata_opts) when metadata_opts != [] do
    Version.from_published(manifest, metadata_opts)
  end

  defp decode_manifest_version(%{} = manifest, _metadata_opts) do
    with {:ok, content_hash} <- Identity.hash_manifest(manifest) do
      Version.new(manifest, manifest_version_id: stable_manifest_version_id(content_hash))
    end
  end

  defp decode_manifest_version(_other, _metadata_opts), do: {:error, :invalid_manifest_json}

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

  defp runner_payload(%Version{} = version) do
    %{
      manifest_version_id: version.manifest_version_id,
      runner_contract_version: version.runner_contract_version
    }
  end

  defp registration_status(%{"data" => data}, key) when is_map(data) and is_binary(key) do
    case get_in(data, [key, "registration", "status"]) || get_in(data, [key, "status"]) ||
           get_in(data, ["registration", "status"]) do
      status when is_binary(status) and status != "" -> status
      _other -> "unknown"
    end
  end

  defp registration_status(_payload, _key), do: "unknown"

  defp maybe_activate(_client, _url, _token, _version, false), do: {:ok, false}

  defp maybe_activate(client, orchestrator_url, service_token, version, true) do
    case client.activate_manifest(orchestrator_url, service_token, version.manifest_version_id) do
      {:ok, _result} -> {:ok, true}
      {:error, _reason} = error -> error
    end
  end

  defp verify_active_manifest(client, orchestrator_url, service_token, version) do
    case client.bootstrap_active_manifest(orchestrator_url, service_token) do
      {:ok, %{"manifest" => %{"manifest_version_id" => id}}}
      when id == version.manifest_version_id ->
        :matched

      {:ok, %{"manifest_version_id" => id}} when id == version.manifest_version_id ->
        :matched

      {:ok, other} ->
        {:mismatch, other}

      {:error, reason} ->
        {:skipped, reason}
    end
  end

  defp required_string(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:missing_required_option, key, value}}
    end
  end
end
