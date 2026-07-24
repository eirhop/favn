defmodule FavnAuthoring.Deployment.ManifestPublication do
  @moduledoc false

  alias Favn.Manifest.{ExecutionPackage, Identity, Publication, Serializer, Version}

  @spec read(Path.t()) :: {:ok, Publication.t()} | {:error, term()}
  def read(path) when is_binary(path) do
    with {:ok, version} <- read_version(path),
         {:ok, packages} <- read_execution_packages(Path.dirname(path)),
         {:ok, publication} <- Publication.from_parts(version, packages) do
      {:ok, publication}
    end
  end

  @spec read_version(Path.t()) :: {:ok, Version.t()} | {:error, term()}
  def read_version(path) when is_binary(path) do
    with {:ok, bytes} <- read_manifest_file(path),
         metadata_opts <- packaged_manifest_opts(path),
         {:ok, version} <- decode_version_bytes(bytes, metadata_opts) do
      Version.verify(version)
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

  defp decode_version_bytes(bytes, metadata_opts) do
    case Serializer.decode_manifest(bytes) do
      {:ok, manifest} when metadata_opts != [] ->
        Version.from_published(manifest, metadata_opts)

      {:ok, manifest} ->
        decode_version(manifest)

      {:error, _reason} ->
        with {:ok, decoded} <- JSON.decode(bytes) do
          decode_version(decoded, metadata_opts)
        end
    end
  end

  defp packaged_manifest_opts(path) do
    directory = Path.dirname(path)

    ["bundle.json", "metadata.json"]
    |> Enum.find_value([], fn filename ->
      case read_metadata_opts(Path.join(directory, filename)) do
        [] -> nil
        opts -> opts
      end
    end)
  end

  defp read_metadata_opts(metadata_path) do
    with {:ok, bytes} <- File.read(metadata_path),
         {:ok, %{"manifest" => %{} = manifest_metadata}} <- JSON.decode(bytes) do
      []
      |> put_metadata_opt(:manifest_version_id, manifest_metadata["manifest_version_id"])
      |> put_metadata_opt(:content_hash, manifest_metadata["content_hash"])
      |> put_metadata_opt(
        :required_runner_release_id,
        manifest_metadata["required_runner_release_id"]
      )
    else
      _other -> []
    end
  end

  defp put_metadata_opt(opts, key, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, key, value)

  defp put_metadata_opt(opts, _key, _value), do: opts

  defp read_manifest_file(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:manifest_read_failed, path, reason}}
    end
  end

  defp decode_version(decoded, metadata_opts \\ [])

  defp decode_version(%{"manifest" => manifest} = envelope, metadata_opts)
       when is_map(manifest) do
    Version.from_published(manifest,
      manifest_version_id:
        Keyword.get(metadata_opts, :manifest_version_id) || envelope["manifest_version_id"],
      content_hash: Keyword.get(metadata_opts, :content_hash) || envelope["content_hash"],
      schema_version: envelope["schema_version"],
      runner_contract_version: envelope["runner_contract_version"],
      required_runner_release_id: envelope["required_runner_release_id"],
      serialization_format: Map.get(envelope, "serialization_format", "json-v1")
    )
  end

  defp decode_version(%{} = manifest, metadata_opts) when metadata_opts != [] do
    Version.from_published(manifest, metadata_opts)
  end

  defp decode_version(%{} = manifest, _metadata_opts) do
    with {:ok, content_hash} <- Identity.hash_manifest(manifest) do
      Version.new(manifest, manifest_version_id: stable_manifest_version_id(content_hash))
    end
  end

  defp decode_version(_other, _metadata_opts), do: {:error, :invalid_manifest_index_json}

  defp stable_manifest_version_id(content_hash),
    do: "mv_" <> String.slice(content_hash, 0, 32)
end
