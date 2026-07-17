defmodule FavnOrchestrator.Storage.ExecutionPackageCodec do
  @moduledoc false

  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Serializer

  @type package_record :: %{
          required(:content_hash) => String.t(),
          required(:asset_module) => String.t(),
          required(:asset_name) => String.t(),
          required(:package_json) => String.t(),
          optional(:inserted_at) => DateTime.t() | nil
        }

  @spec to_record(ExecutionPackage.t()) :: {:ok, package_record()} | {:error, term()}
  def to_record(%ExecutionPackage{} = package) do
    with {:ok, canonical} <- ExecutionPackage.verify(package),
         {:ok, package_json} <- Serializer.encode_manifest(canonical) do
      {module, name} = canonical.asset_ref

      {:ok,
       %{
         content_hash: canonical.content_hash,
         asset_module: Atom.to_string(module),
         asset_name: Atom.to_string(name),
         package_json: package_json,
         inserted_at: DateTime.utc_now()
       }}
    end
  end

  @spec from_record(package_record()) :: {:ok, ExecutionPackage.t()} | {:error, term()}
  def from_record(%{package_json: package_json}) when is_binary(package_json) do
    with {:ok, raw} <- Serializer.decode_manifest(package_json) do
      ExecutionPackage.from_published(raw)
    end
  end

  def from_record(record), do: {:error, {:invalid_execution_package_record, record}}
end
