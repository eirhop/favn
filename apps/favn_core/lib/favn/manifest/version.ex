defmodule Favn.Manifest.Version do
  @moduledoc """
  Immutable pinned manifest version envelope.

  Runtime work may retain only the version identity after compiling the
  manifest into a bounded execution index. In that form `manifest` is `nil`;
  all immutable version and runner-release fields remain available.

  Versions minted without an explicit `:manifest_version_id` use `mv_` plus the
  full lowercase SHA-256 `content_hash`. Explicit IDs remain supported for
  imported and already-published envelopes. Verification preserves those IDs;
  `content_hash` remains the canonical deduplication identity.
  """

  alias Favn.Manifest
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Rehydrate
  alias Favn.Manifest.Serializer

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          runner_releases: Favn.RunnerPool.releases(),
          serialization_format: String.t(),
          manifest: Manifest.t() | nil,
          inserted_at: DateTime.t() | nil
        }

  defstruct [
    :manifest_version_id,
    :content_hash,
    :schema_version,
    :runner_contract_version,
    :runner_releases,
    :manifest,
    inserted_at: nil,
    serialization_format: "json-v1"
  ]

  @doc "Drops the manifest payload while retaining its immutable identity."
  @spec identity(t()) :: t()
  def identity(%__MODULE__{} = version), do: %{version | manifest: nil}

  @doc "Returns the exact immutable release bound to a logical runner pool."
  @spec release_for_pool(t(), atom() | String.t()) :: {:ok, String.t()} | {:error, term()}
  def release_for_pool(%__MODULE__{runner_releases: releases}, pool) when is_atom(pool) do
    with {:ok, pool_name} <- Favn.RunnerPool.encode(pool) do
      Favn.RunnerPool.fetch_release(releases, pool_name)
    end
  end

  def release_for_pool(%__MODULE__{runner_releases: releases}, pool) when is_binary(pool),
    do: Favn.RunnerPool.fetch_release(releases, pool)

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
          | {:runner_releases, Favn.RunnerPool.releases()}

  @type error ::
          {:invalid_manifest_version_id, term()}
          | {:invalid_content_hash, term()}
          | {:invalid_serialization_format, term()}
          | {:unknown_opt, atom()}
          | {:manifest_content_hash_mismatch, String.t(), String.t()}
          | {:manifest_schema_version_mismatch, pos_integer(), pos_integer()}
          | {:manifest_runner_contract_version_mismatch, pos_integer(), pos_integer()}
          | {:invalid_runner_releases, term()}
          | {:manifest_runner_releases_mismatch, map(), map()}
          | {:legacy_manifest_field, :sql_execution}
          | Rehydrate.error()
          | Serializer.error()
          | Compatibility.error()
          | Identity.error()

  @doc """
  Canonicalizes a manifest and pins it in an immutable version envelope.

  By default the manifest version ID is `mv_` plus the full lowercase SHA-256
  content hash. Pass `:manifest_version_id` only when preserving an explicit
  external or previously published identity.
  """
  @spec new(map() | struct(), [opt()]) :: {:ok, t()} | {:error, error()}
  def new(manifest, opts \\ []) when is_list(opts) do
    serialization_format = Keyword.get(opts, :serialization_format, "json-v1")

    with :ok <- validate_opts(opts),
         :ok <- reject_legacy_execution_payload(manifest),
         {:ok, canonical_manifest} <- Rehydrate.manifest(manifest),
         {:ok, stable_manifest} <- canonicalize_manifest(canonical_manifest),
         :ok <- Compatibility.validate_manifest(stable_manifest),
         {:ok, schema_version} <- read_field(stable_manifest, :schema_version),
         {:ok, runner_contract_version} <-
           read_field(stable_manifest, :runner_contract_version),
         {:ok, runner_releases} <- read_field(stable_manifest, :runner_releases),
         :ok <- validate_serialization_format(serialization_format),
         {:ok, content_hash} <-
           Identity.hash_manifest(stable_manifest,
             algorithm: Keyword.get(opts, :hash_algorithm, :sha256)
           ),
         {:ok, manifest_version_id} <- resolve_manifest_version_id(opts, content_hash) do
      {:ok,
       %__MODULE__{
         manifest_version_id: manifest_version_id,
         content_hash: content_hash,
         schema_version: schema_version,
         runner_contract_version: runner_contract_version,
         runner_releases: runner_releases,
         serialization_format: serialization_format,
         manifest: stable_manifest,
         inserted_at: Keyword.get(opts, :inserted_at)
       }}
    end
  end

  @doc """
  Builds a manifest version from an already-published version envelope.

  This function verifies that the supplied manifest payload still matches the
  supplied content hash. It is intended for services that receive a manifest
  version created elsewhere and must validate, not mint, the manifest identity.
  The publication envelope must include `:runner_releases`, which is
  matched exactly against the canonical manifest payload.
  """
  @spec from_published(map() | struct(), [published_opt()]) :: {:ok, t()} | {:error, error()}
  def from_published(manifest, opts) when is_list(opts) do
    with :ok <- validate_published_opts(opts),
         {:ok, manifest_version_id} <- fetch_manifest_version_id(opts),
         {:ok, expected_hash} <- fetch_content_hash(opts),
         {:ok, expected_runner_releases} <- fetch_runner_releases(opts),
         {:ok, version} <-
           new(manifest,
             manifest_version_id: manifest_version_id,
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
           ),
         :ok <-
           match_runner_releases(version.runner_releases, expected_runner_releases) do
      {:ok, version}
    end
  end

  @doc """
  Verifies that a manifest version envelope is internally consistent.
  """
  @spec verify(t()) :: {:ok, t()} | {:error, error()}
  def verify(%__MODULE__{} = version) do
    with :ok <- reject_legacy_execution_payload(version.manifest),
         :ok <- validate_manifest_version_id(version.manifest_version_id),
         :ok <- validate_content_hash(version.content_hash),
         :ok <- validate_serialization_format(version.serialization_format),
         {:ok, canonical_manifest} <- Rehydrate.manifest(version.manifest),
         {:ok, stable_manifest} <- canonicalize_manifest(canonical_manifest),
         :ok <- Compatibility.validate_manifest(stable_manifest),
         {:ok, schema_version} <- read_field(stable_manifest, :schema_version),
         :ok <- match_schema_version(schema_version, version.schema_version),
         {:ok, runner_contract_version} <-
           read_field(stable_manifest, :runner_contract_version),
         :ok <-
           match_runner_contract_version(
             runner_contract_version,
             version.runner_contract_version
           ),
         {:ok, runner_releases} <- read_field(stable_manifest, :runner_releases),
         :ok <- match_runner_releases(runner_releases, version.runner_releases),
         {:ok, computed_hash} <- Identity.hash_manifest(stable_manifest),
         :ok <- match_content_hash(computed_hash, version.content_hash) do
      {:ok, %{version | manifest: stable_manifest}}
    end
  end

  defp canonicalize_manifest(manifest) do
    with {:ok, encoded} <- Serializer.encode_manifest(manifest),
         {:ok, decoded} <- Serializer.decode_manifest(encoded) do
      Rehydrate.manifest(decoded)
    end
  end

  defp reject_legacy_execution_payload(%{manifest: manifest}),
    do: reject_legacy_execution_payload(manifest)

  defp reject_legacy_execution_payload(manifest) when is_map(manifest) do
    assets = Map.get(manifest, :assets, Map.get(manifest, "assets", []))

    if Enum.any?(assets, fn asset ->
         is_map(asset) and
           (Map.has_key?(asset, :sql_execution) or Map.has_key?(asset, "sql_execution"))
       end) do
      {:error, {:legacy_manifest_field, :sql_execution}}
    else
      :ok
    end
  end

  defp reject_legacy_execution_payload(_manifest), do: :ok

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
      :runner_releases,
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
      value when is_binary(value) and value != "" ->
        with :ok <- validate_content_hash(value), do: {:ok, value}

      value ->
        {:error, {:invalid_content_hash, value}}
    end
  end

  defp fetch_manifest_version_id(opts) do
    case Keyword.get(opts, :manifest_version_id) do
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:invalid_manifest_version_id, value}}
    end
  end

  defp fetch_runner_releases(opts) do
    value = Keyword.get(opts, :runner_releases)

    case Favn.RunnerPool.validate_releases(value) do
      :ok -> {:ok, value}
      {:error, _reason} -> {:error, {:invalid_runner_releases, value}}
    end
  end

  defp validate_manifest_version_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_manifest_version_id(value), do: {:error, {:invalid_manifest_version_id, value}}

  defp validate_content_hash(value) when is_binary(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, {:invalid_content_hash, value}}
  end

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

  defp match_runner_releases(value, value), do: :ok

  defp match_runner_releases(actual, expected),
    do: {:error, {:manifest_runner_releases_mismatch, expected, actual}}

  defp resolve_manifest_version_id(opts, content_hash) do
    case Keyword.fetch(opts, :manifest_version_id) do
      {:ok, manifest_version_id} ->
        with :ok <- validate_manifest_version_id(manifest_version_id),
             do: {:ok, manifest_version_id}

      :error ->
        {:ok, "mv_" <> content_hash}
    end
  end
end
