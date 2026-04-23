defmodule Favn.Manifest.Version do
  @moduledoc """
  Immutable pinned manifest version envelope.
  """

  alias Favn.Manifest
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Rehydrate

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          serialization_format: String.t(),
          manifest: Manifest.t(),
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
          | {:serialization_format, String.t()}
          | {:inserted_at, DateTime.t()}
          | {:hash_algorithm, :sha256}

  @type error ::
          {:invalid_manifest_version_id, term()}
          | {:invalid_serialization_format, term()}
          | {:unknown_opt, atom()}
          | Rehydrate.error()
          | Compatibility.error()
          | Identity.error()

  @spec new(map() | struct(), [opt()]) :: {:ok, t()} | {:error, error()}
  def new(manifest, opts \\ []) when is_list(opts) do
    manifest_version_id = Keyword.get(opts, :manifest_version_id, default_manifest_version_id())
    serialization_format = Keyword.get(opts, :serialization_format, "json-v1")

    with :ok <- validate_opts(opts),
         {:ok, canonical_manifest} <- Rehydrate.manifest(manifest),
         :ok <- Compatibility.validate_manifest(canonical_manifest),
         {:ok, schema_version} <- read_field(canonical_manifest, :schema_version),
         {:ok, runner_contract_version} <-
           read_field(canonical_manifest, :runner_contract_version),
         :ok <- validate_manifest_version_id(manifest_version_id),
         :ok <- validate_serialization_format(serialization_format),
         {:ok, content_hash} <-
           Identity.hash_manifest(canonical_manifest,
             algorithm: Keyword.get(opts, :hash_algorithm, :sha256)
           ) do
      {:ok,
       %__MODULE__{
         manifest_version_id: manifest_version_id,
         content_hash: content_hash,
         schema_version: schema_version,
         runner_contract_version: runner_contract_version,
         serialization_format: serialization_format,
         manifest: canonical_manifest,
         inserted_at: Keyword.get(opts, :inserted_at)
       }}
    end
  end

  defp read_field(value, field) do
    case Map.fetch(value, field) do
      {:ok, field_value} -> {:ok, field_value}
      :error -> {:error, {:missing_manifest_field, field}}
    end
  end

  defp validate_opts(opts) do
    allowed = [:manifest_version_id, :serialization_format, :inserted_at, :hash_algorithm]

    case Enum.find(opts, fn {key, _value} -> key not in allowed end) do
      nil -> :ok
      {key, _value} -> {:error, {:unknown_opt, key}}
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
