defmodule Favn.Manifest.Version do
  @moduledoc """
  Immutable pinned manifest version envelope.
  """

  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Identity

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          serialization_format: String.t(),
          manifest: map() | struct(),
          inserted_at: DateTime.t() | nil
        }

  defstruct [
    :manifest_version_id,
    :content_hash,
    :schema_version,
    :runner_contract_version,
    :manifest,
    inserted_at: nil,
    serialization_format: "json-v1"
  ]

  @type opt ::
          {:manifest_version_id, String.t()}
          | {:schema_version, pos_integer()}
          | {:runner_contract_version, pos_integer()}
          | {:serialization_format, String.t()}
          | {:inserted_at, DateTime.t()}
          | {:hash_algorithm, :sha256}

  @type error ::
          {:invalid_manifest_version_id, term()}
          | {:invalid_serialization_format, term()}
          | Favn.Manifest.Compatibility.error()
          | Favn.Manifest.Identity.error()

  @spec new(map() | struct(), [opt()]) :: {:ok, t()} | {:error, error()}
  def new(manifest, opts \\ []) when is_list(opts) do
    schema_version = Keyword.get(opts, :schema_version, Compatibility.current_schema_version())

    runner_contract_version =
      Keyword.get(opts, :runner_contract_version, Compatibility.current_runner_contract_version())

    manifest_version_id = Keyword.get(opts, :manifest_version_id, default_manifest_version_id())
    serialization_format = Keyword.get(opts, :serialization_format, "json-v1")

    with :ok <- Compatibility.validate_schema_version(schema_version),
         :ok <- Compatibility.validate_runner_contract_version(runner_contract_version),
         :ok <- validate_manifest_version_id(manifest_version_id),
         :ok <- validate_serialization_format(serialization_format),
         {:ok, content_hash} <-
           Identity.hash_manifest(manifest,
             algorithm: Keyword.get(opts, :hash_algorithm, :sha256)
           ) do
      {:ok,
       %__MODULE__{
         manifest_version_id: manifest_version_id,
         content_hash: content_hash,
         schema_version: schema_version,
         runner_contract_version: runner_contract_version,
         serialization_format: serialization_format,
         manifest: manifest,
         inserted_at: Keyword.get(opts, :inserted_at)
       }}
    end
  end

  defp validate_manifest_version_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_manifest_version_id(value), do: {:error, {:invalid_manifest_version_id, value}}

  defp validate_serialization_format(value) when is_binary(value) and value != "", do: :ok
  defp validate_serialization_format(value), do: {:error, {:invalid_serialization_format, value}}

  defp default_manifest_version_id do
    "mv_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
