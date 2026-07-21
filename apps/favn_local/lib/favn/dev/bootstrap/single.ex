defmodule Favn.Dev.Bootstrap.Single do
  @moduledoc """
  API-driven bootstrap workflow for a PostgreSQL-backed single-node runtime.

  The workflow does not write storage directly. It verifies orchestrator service
  credentials, validates `manifest-index.json` and its sibling immutable
  execution packages as a `Favn.Manifest.Publication`, then publishes and
  activates that index before asking the orchestrator to register the persisted
  index with the local runner.
  """

  alias Favn.Dev.OrchestratorClient
  alias Favn.Manifest.Identity
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Publication
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  @type opts :: [
          manifest_path: Path.t(),
          orchestrator_url: String.t(),
          service_token: String.t(),
          workspace_id: String.t(),
          operator_username: String.t(),
          operator_password: String.t(),
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
         {:ok, workspace_id} <- required_string(opts, :workspace_id),
         :ok <- client.verify_service_token(orchestrator_url, service_token),
         {:ok, session_context} <-
           maybe_operator_session(client, orchestrator_url, service_token, workspace_id, opts),
         {:ok, publication} <- read_manifest_publication(manifest_path),
         version <- publication.version,
         {:ok, registration} <-
           client.publish_manifest(orchestrator_url, service_token, publication, session_context),
         {:ok, canonical_manifest_version_id} <-
           canonical_manifest_version_id(registration, version.manifest_version_id),
         canonical_version <-
           %{version | manifest_version_id: canonical_manifest_version_id},
         manifest_registration <- registration_status(registration, "manifest"),
         {:ok, activated?} <-
           maybe_activate(
             client,
             orchestrator_url,
             service_token,
             session_context,
             canonical_version,
             activate?
           ),
         {:ok, runner} <-
           client.register_runner(
             orchestrator_url,
             service_token,
             session_context,
             runner_payload(canonical_version)
           ),
         runner_registration <- registration_status(runner, "registration"),
         verification <-
           verify_active_manifest(
             client,
             orchestrator_url,
             service_token,
             session_context,
             canonical_version
           ) do
      {:ok,
       %{
         manifest_version_id: canonical_version.manifest_version_id,
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

  @doc false
  @spec read_manifest_publication(Path.t()) :: {:ok, Publication.t()} | {:error, term()}
  def read_manifest_publication(path) when is_binary(path) do
    with {:ok, version} <- read_manifest_version(path),
         {:ok, packages} <- read_execution_packages(Path.dirname(path)),
         {:ok, publication} <- Publication.from_parts(version, packages) do
      {:ok, publication}
    end
  end

  defp read_execution_packages(artifact_dir) do
    artifact_dir
    |> Path.join("execution-packages/*.json")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, []}, fn path, {:ok, packages} ->
      with {:ok, encoded} <- File.read(path),
           {:ok, decoded} <- Serializer.decode_manifest(encoded),
           {:ok, package} <- ExecutionPackage.from_published(decoded) do
        {:cont, {:ok, [package | packages]}}
      else
        {:error, reason} -> {:halt, {:error, {path, reason}}}
      end
    end)
    |> case do
      {:ok, packages} -> {:ok, Enum.reverse(packages)}
      {:error, reason} -> {:error, reason}
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
      |> put_metadata_opt(
        :required_runner_release_id,
        Map.get(manifest_metadata, "required_runner_release_id")
      )
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
        Keyword.get(metadata_opts, :manifest_version_id) ||
          Map.get(envelope, "manifest_version_id"),
      content_hash:
        Keyword.get(metadata_opts, :content_hash) || Map.get(envelope, "content_hash"),
      schema_version: Map.get(envelope, "schema_version"),
      runner_contract_version: Map.get(envelope, "runner_contract_version"),
      required_runner_release_id: Map.get(envelope, "required_runner_release_id"),
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

  defp decode_manifest_version(_other, _metadata_opts),
    do: {:error, :invalid_manifest_index_json}

  defp stable_manifest_version_id(content_hash) when is_binary(content_hash) do
    "mv_" <> String.slice(content_hash, 0, 32)
  end

  defp runner_payload(%Version{} = version) do
    %{
      manifest_version_id: version.manifest_version_id,
      runner_contract_version: version.runner_contract_version,
      required_runner_release_id: version.required_runner_release_id
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

  defp canonical_manifest_version_id(registration, packaged_manifest_version_id) do
    canonical_id =
      get_in(registration, ["data", "registration", "canonical_manifest_version_id"]) ||
        get_in(registration, ["data", "manifest", "registration", "canonical_manifest_version_id"]) ||
        get_in(registration, ["registration", "canonical_manifest_version_id"]) ||
        packaged_manifest_version_id

    if is_binary(canonical_id) and canonical_id != "" do
      {:ok, canonical_id}
    else
      {:error, :invalid_manifest_registration}
    end
  end

  defp maybe_operator_session(client, orchestrator_url, service_token, workspace_id, opts) do
    with {:ok, username} <- required_string(opts, :operator_username),
         {:ok, password} <- required_string(opts, :operator_password) do
      client.password_login(orchestrator_url, service_token, workspace_id, username, password)
    end
  end

  defp maybe_activate(_client, _url, _token, _session_context, _version, false), do: {:ok, false}

  defp maybe_activate(client, orchestrator_url, service_token, session_context, version, true) do
    case client.activate_manifest(
           orchestrator_url,
           service_token,
           version.manifest_version_id,
           session_context
         ) do
      {:ok, _result} -> {:ok, true}
      {:error, _reason} = error -> error
    end
  end

  defp verify_active_manifest(client, orchestrator_url, service_token, session_context, version) do
    case client.bootstrap_active_manifest(orchestrator_url, service_token, session_context) do
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
