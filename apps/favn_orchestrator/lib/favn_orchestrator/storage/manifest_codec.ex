defmodule FavnOrchestrator.Storage.ManifestCodec do
  @moduledoc false

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  @type record :: %{
          required(:manifest_version_id) => String.t(),
          required(:content_hash) => String.t(),
          required(:schema_version) => pos_integer(),
          required(:runner_contract_version) => pos_integer(),
          required(:serialization_format) => String.t(),
          required(:manifest_json) => String.t(),
          optional(:inserted_at) => DateTime.t() | nil
        }

  @spec to_record(Version.t()) :: {:ok, record()} | {:error, term()}
  def to_record(%Version{} = version) do
    with {:ok, manifest_json} <- Serializer.encode_manifest(version.manifest) do
      {:ok,
       %{
         manifest_version_id: version.manifest_version_id,
         content_hash: version.content_hash,
         schema_version: version.schema_version,
         runner_contract_version: version.runner_contract_version,
         serialization_format: version.serialization_format,
         manifest_json: manifest_json,
         inserted_at: version.inserted_at
       }}
    end
  end

  @spec from_record(record()) :: {:ok, Version.t()} | {:error, term()}
  def from_record(record) when is_map(record) do
    with {:ok, manifest_version_id} <- fetch_non_empty_binary(record, :manifest_version_id),
         {:ok, content_hash} <- fetch_non_empty_binary(record, :content_hash),
         {:ok, serialization_format} <- fetch_non_empty_binary(record, :serialization_format),
         {:ok, manifest_json} <- fetch_non_empty_binary(record, :manifest_json),
         {:ok, raw_manifest} <- Serializer.decode_manifest(manifest_json) do
      Version.from_published(raw_manifest,
        manifest_version_id: manifest_version_id,
        content_hash: content_hash,
        schema_version: Map.get(record, :schema_version),
        runner_contract_version: Map.get(record, :runner_contract_version),
        serialization_format: serialization_format,
        inserted_at: Map.get(record, :inserted_at)
      )
    end
  end

  defp fetch_non_empty_binary(record, key) do
    case Map.get(record, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:invalid_manifest_record_field, key, value}}
    end
  end
end
