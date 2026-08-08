defmodule FavnOrchestrator.TestSupport.ManifestRecord do
  @moduledoc false

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version

  @spec to_record(Version.t()) :: {:ok, map()} | {:error, term()}
  def to_record(%Version{} = version) do
    with {:ok, manifest_index_json} <- Serializer.encode_manifest(version.manifest) do
      {:ok,
       %{
         manifest_version_id: version.manifest_version_id,
         content_hash: version.content_hash,
         schema_version: version.schema_version,
         runner_contract_version: version.runner_contract_version,
         runner_releases: version.runner_releases,
         serialization_format: version.serialization_format,
         manifest_index_json: manifest_index_json,
         inserted_at: version.inserted_at
       }}
    end
  end
end
