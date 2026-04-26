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

  @type published_opt ::
          opt()
          | {:content_hash, String.t()}
          | {:schema_version, pos_integer()}
          | {:runner_contract_version, pos_integer()}

  @type error ::
          {:invalid_manifest_version_id, term()}
          | {:invalid_content_hash, term()}
          | {:invalid_serialization_format, term()}
          | {:unknown_opt, atom()}
          | {:manifest_content_hash_mismatch, String.t(), String.t()}
          | {:manifest_schema_version_mismatch, pos_integer(), pos_integer()}
          | {:manifest_runner_contract_version_mismatch, pos_integer(), pos_integer()}
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

  @doc """
  Builds a manifest version from an already-published version envelope.

  This function verifies that the supplied manifest payload still matches the
  supplied content hash. It is intended for services that receive a manifest
  version created elsewhere and must validate, not mint, the manifest identity.
  """
  @spec from_published(map() | struct(), [published_opt()]) :: {:ok, t()} | {:error, error()}
  def from_published(manifest, opts) when is_list(opts) do
    with :ok <- validate_published_opts(opts),
         {:ok, expected_hash} <- fetch_content_hash(opts),
         {:ok, version} <-
           new(manifest,
             manifest_version_id: Keyword.get(opts, :manifest_version_id),
             serialization_format: Keyword.get(opts, :serialization_format, "json-v1"),
             inserted_at: Keyword.get(opts, :inserted_at),
             hash_algorithm: Keyword.get(opts, :hash_algorithm, :sha256)
           ),
         :ok <- match_content_hash(version.content_hash, expected_hash),
         :ok <-
           match_optional_schema_version(
             version.schema_version,
             Keyword.get(opts, :schema_version)
           ),
         :ok <-
           match_optional_runner_contract_version(
             version.runner_contract_version,
             Keyword.get(opts, :runner_contract_version)
           ) do
      {:ok, version}
    end
  end

  @doc """
  Verifies that a manifest version envelope is internally consistent.
  """
  @spec verify(t()) :: {:ok, t()} | {:error, error()}
  def verify(%__MODULE__{} = version) do
    with :ok <- validate_manifest_version_id(version.manifest_version_id),
         :ok <- validate_content_hash(version.content_hash),
         :ok <- validate_serialization_format(version.serialization_format),
         {:ok, canonical_manifest} <- Rehydrate.manifest(version.manifest),
         :ok <- Compatibility.validate_manifest(canonical_manifest),
         {:ok, schema_version} <- read_field(canonical_manifest, :schema_version),
         :ok <- match_schema_version(schema_version, version.schema_version),
         {:ok, runner_contract_version} <-
           read_field(canonical_manifest, :runner_contract_version),
         :ok <-
           match_runner_contract_version(
             runner_contract_version,
             version.runner_contract_version
           ),
         {:ok, computed_hash} <- Identity.hash_manifest(canonical_manifest),
         :ok <- match_content_hash(computed_hash, version.content_hash) do
      {:ok, %{version | manifest: canonical_manifest}}
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

  defp validate_published_opts(opts) do
    allowed = [
      :manifest_version_id,
      :content_hash,
      :schema_version,
      :runner_contract_version,
      :serialization_format,
      :inserted_at,
      :hash_algorithm
    ]

    case Enum.find(opts, fn {key, _value} -> key not in allowed end) do
      nil -> :ok
      {key, _value} -> {:error, {:unknown_opt, key}}
    end
  end

  defp fetch_content_hash(opts) do
    case Keyword.get(opts, :content_hash) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:invalid_content_hash, value}}
    end
  end

  defp validate_manifest_version_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_manifest_version_id(value), do: {:error, {:invalid_manifest_version_id, value}}

  defp validate_content_hash(value) when is_binary(value) and byte_size(value) == 64, do: :ok
  defp validate_content_hash(value), do: {:error, {:invalid_content_hash, value}}

  defp validate_serialization_format(value) when is_binary(value) and value != "", do: :ok
  defp validate_serialization_format(value), do: {:error, {:invalid_serialization_format, value}}

  defp match_content_hash(computed, expected) when computed == expected, do: :ok

  defp match_content_hash(computed, expected),
    do: {:error, {:manifest_content_hash_mismatch, expected, computed}}

  defp match_schema_version(value, value), do: :ok

  defp match_schema_version(actual, expected),
    do: {:error, {:manifest_schema_version_mismatch, expected, actual}}

  defp match_optional_schema_version(_actual, nil), do: :ok
  defp match_optional_schema_version(actual, expected), do: match_schema_version(actual, expected)

  defp match_runner_contract_version(value, value), do: :ok

  defp match_runner_contract_version(actual, expected),
    do: {:error, {:manifest_runner_contract_version_mismatch, expected, actual}}

  defp match_optional_runner_contract_version(_actual, nil), do: :ok

  defp match_optional_runner_contract_version(actual, expected),
    do: match_runner_contract_version(actual, expected)

  defp default_manifest_version_id do
    "mv_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
