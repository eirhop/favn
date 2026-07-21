defmodule Favn.RunnerRelease do
  @moduledoc """
  Immutable identity of the exact customer runtime required by a manifest.

  A runner release ID fingerprints executable customer modules, runtime
  applications, plugins, language/runtime versions, and the runner protocol.
  It deliberately excludes image tags and digests, checkout paths, Git branch,
  build time, and manifest content. The OCI digest identifies the packaged
  image; `runner_release_id` identifies its logical runtime compatibility.

  Descriptors are serialized as canonical JSON and can be validated without
  loading customer modules or creating atoms from artifact data.
  """

  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Serializer
  alias Favn.RunnerRelease.ApplicationFingerprint
  alias Favn.RunnerRelease.ModuleFingerprint
  alias Favn.RunnerRelease.PluginFingerprint
  alias Favn.RunnerRelease.Validation

  @schema_version 1
  @target "linux/amd64"
  @build_profile "prod"
  @runner_release_id ~r/\Arr_[0-9a-f]{64}\z/
  @max_build_metadata_bytes 65_536
  @fallback_favn_version "0.5.0-dev"
  @descriptor_fields [
    :schema_version,
    :favn_version,
    :runner_contract_version,
    :elixir_version,
    :otp_release,
    :target,
    :runtime_code_digest,
    :runtime_dependency_digest,
    :runtime_modules,
    :runtime_applications,
    :plugins,
    :build_profile,
    :runner_release_id,
    :build_metadata
  ]

  @enforce_keys [
    :schema_version,
    :favn_version,
    :runner_contract_version,
    :elixir_version,
    :otp_release,
    :target,
    :runtime_code_digest,
    :runtime_dependency_digest,
    :runtime_modules,
    :runtime_applications,
    :plugins,
    :build_profile,
    :runner_release_id
  ]
  defstruct [
    :schema_version,
    :favn_version,
    :runner_contract_version,
    :elixir_version,
    :otp_release,
    :target,
    :runtime_code_digest,
    :runtime_dependency_digest,
    :runner_release_id,
    runtime_modules: [],
    runtime_applications: [],
    plugins: [],
    build_profile: @build_profile,
    build_metadata: %{}
  ]

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          favn_version: String.t(),
          runner_contract_version: pos_integer(),
          elixir_version: String.t(),
          otp_release: String.t(),
          target: String.t(),
          runtime_code_digest: String.t(),
          runtime_dependency_digest: String.t(),
          runtime_modules: [ModuleFingerprint.t()],
          runtime_applications: [ApplicationFingerprint.t()],
          plugins: [PluginFingerprint.t()],
          build_profile: String.t(),
          runner_release_id: String.t(),
          build_metadata: map()
        }

  @type error ::
          {:invalid_runner_release, :expected_map}
          | {:invalid_runner_release_json, term()}
          | {:missing_runner_release_field, atom()}
          | {:invalid_runner_release_field, atom(), atom()}
          | {:unsupported_runner_release_schema, term(), pos_integer()}
          | {:unsupported_runner_contract, term(), pos_integer()}
          | {:unsupported_favn_version, term(), String.t()}
          | {:duplicate_runner_release_entry, atom(), String.t()}
          | {:runtime_code_digest_mismatch, String.t(), term()}
          | {:runtime_dependency_digest_mismatch, String.t(), term()}
          | {:runner_release_id_mismatch, String.t(), term()}

  @doc "Returns the only supported runner descriptor schema version."
  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @schema_version

  @doc "Returns the normalized target supported by the first runner release."
  @spec current_target() :: String.t()
  def current_target, do: @target

  @doc "Returns the Favn application version used for compatibility checks."
  @spec current_favn_version() :: String.t()
  def current_favn_version do
    :favn_core
    |> Application.spec(:vsn)
    |> case do
      nil -> @fallback_favn_version
      version -> to_string(version)
    end
  end

  @doc """
  Builds a canonical descriptor and computes its `runner_release_id`.

  Callers may omit both aggregate digests and the release ID; they are derived
  from the normalized entries. If supplied, all three values must match the
  canonical calculation.
  """
  @spec new(map()) :: {:ok, t()} | {:error, error()}
  def new(attrs) when is_map(attrs) do
    with {:ok, schema_version} <- schema_version(attrs),
         {:ok, favn_version} <- favn_version(attrs),
         {:ok, runner_contract_version} <- runner_contract_version(attrs),
         {:ok, elixir_version} <- semantic_version(attrs, :elixir_version),
         {:ok, otp_release} <- otp_release(attrs),
         {:ok, target} <- exact_string(attrs, :target, @target),
         {:ok, runtime_modules} <-
           normalize_entries(attrs, :runtime_modules, &ModuleFingerprint.new/1, :module),
         {:ok, runtime_applications} <-
           normalize_entries(
             attrs,
             :runtime_applications,
             &ApplicationFingerprint.new/1,
             :application
           ),
         {:ok, plugins} <-
           normalize_entries(attrs, :plugins, &PluginFingerprint.new/1, :plugin),
         {:ok, runtime_code_digest} <- code_digest(runtime_modules),
         :ok <- match_optional_digest(attrs, :runtime_code_digest, runtime_code_digest),
         {:ok, runtime_dependency_digest} <-
           dependency_digest(runtime_applications, plugins),
         :ok <-
           match_optional_digest(
             attrs,
             :runtime_dependency_digest,
             runtime_dependency_digest
           ),
         {:ok, build_profile} <- exact_string(attrs, :build_profile, @build_profile),
         {:ok, build_metadata} <- build_metadata(attrs) do
      descriptor = %__MODULE__{
        schema_version: schema_version,
        favn_version: favn_version,
        runner_contract_version: runner_contract_version,
        elixir_version: elixir_version,
        otp_release: otp_release,
        target: target,
        runtime_code_digest: runtime_code_digest,
        runtime_dependency_digest: runtime_dependency_digest,
        runtime_modules: runtime_modules,
        runtime_applications: runtime_applications,
        plugins: plugins,
        build_profile: build_profile,
        runner_release_id: "",
        build_metadata: build_metadata
      }

      runner_release_id = calculate_id(descriptor)

      with :ok <- match_optional_id(attrs, runner_release_id) do
        {:ok, %{descriptor | runner_release_id: runner_release_id}}
      end
    end
  end

  def new(_attrs), do: {:error, {:invalid_runner_release, :expected_map}}

  @doc "Validates and canonicalizes a descriptor, including its self-hash."
  @spec verify(map() | t()) :: {:ok, t()} | {:error, error()}
  def verify(value) when is_map(value) do
    with :ok <- require_descriptor_fields(value),
         {:ok, release_id} <- Validation.fetch(value, :runner_release_id),
         :ok <- validate_id_syntax(release_id) do
      new(value)
    end
  end

  def verify(_value), do: {:error, {:invalid_runner_release, :expected_map}}

  @doc "Decodes and verifies a canonical runner release JSON document."
  @spec decode(binary()) :: {:ok, t()} | {:error, error()}
  def decode(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, value} when is_map(value) -> verify(value)
      {:ok, _value} -> {:error, {:invalid_runner_release_json, :invalid_root}}
      {:error, reason} -> {:error, {:invalid_runner_release_json, reason}}
    end
  end

  def decode(_json), do: {:error, {:invalid_runner_release_json, :expected_binary}}

  @doc "Encodes a verified descriptor as canonical JSON."
  @spec encode(map() | t()) :: {:ok, binary()} | {:error, error() | Serializer.error()}
  def encode(value) do
    with {:ok, descriptor} <- verify(value) do
      Serializer.encode_canonical(serialized_payload(descriptor))
    end
  end

  @doc "Returns the canonical identity payload used to calculate the release ID."
  @spec identity_payload(t()) :: map()
  def identity_payload(%__MODULE__{} = descriptor) do
    %{
      "schema_version" => descriptor.schema_version,
      "favn_version" => descriptor.favn_version,
      "runner_contract_version" => descriptor.runner_contract_version,
      "elixir_version" => descriptor.elixir_version,
      "otp_release" => descriptor.otp_release,
      "target" => descriptor.target,
      "runtime_code_digest" => descriptor.runtime_code_digest,
      "runtime_dependency_digest" => descriptor.runtime_dependency_digest,
      "runtime_modules" =>
        Enum.map(descriptor.runtime_modules, &ModuleFingerprint.identity_payload/1),
      "runtime_applications" =>
        Enum.map(
          descriptor.runtime_applications,
          &ApplicationFingerprint.identity_payload/1
        ),
      "plugins" => Enum.map(descriptor.plugins, &PluginFingerprint.identity_payload/1),
      "build_profile" => descriptor.build_profile
    }
  end

  @doc "Returns the canonical JSON bytes hashed by `runner_release_id`."
  @spec identity_json(t()) :: binary()
  def identity_json(%__MODULE__{} = descriptor) do
    descriptor
    |> identity_payload()
    |> Serializer.encode_canonical!()
  end

  @doc "Calculates the aggregate runtime module digest."
  @spec code_digest([ModuleFingerprint.t()]) :: {:ok, String.t()} | {:error, error()}
  def code_digest(modules) when is_list(modules) do
    with {:ok, modules} <- normalize_existing_entries(modules, &ModuleFingerprint.new/1, :module) do
      modules
      |> Enum.map(&ModuleFingerprint.identity_payload/1)
      |> canonical_sha256()
    end
  end

  def code_digest(_modules),
    do: {:error, {:invalid_runner_release_field, :runtime_modules, :expected_list}}

  @doc "Calculates the aggregate runtime application/plugin digest."
  @spec dependency_digest([ApplicationFingerprint.t()], [PluginFingerprint.t()]) ::
          {:ok, String.t()} | {:error, error()}
  def dependency_digest(applications, plugins)
      when is_list(applications) and is_list(plugins) do
    with {:ok, applications} <-
           normalize_existing_entries(
             applications,
             &ApplicationFingerprint.new/1,
             :application
           ),
         {:ok, plugins} <-
           normalize_existing_entries(plugins, &PluginFingerprint.new/1, :plugin) do
      canonical_sha256(%{
        "runtime_applications" =>
          Enum.map(applications, &ApplicationFingerprint.identity_payload/1),
        "plugins" => Enum.map(plugins, &PluginFingerprint.identity_payload/1)
      })
    end
  end

  def dependency_digest(_applications, _plugins),
    do: {:error, {:invalid_runner_release_field, :runtime_applications, :expected_list}}

  defp serialized_payload(descriptor) do
    descriptor
    |> identity_payload()
    |> Map.put("runner_release_id", descriptor.runner_release_id)
    |> Map.put("build_metadata", descriptor.build_metadata)
  end

  defp calculate_id(descriptor) do
    "rr_" <> sha256(identity_json(descriptor))
  end

  defp canonical_sha256(value) do
    {:ok, value |> Serializer.encode_canonical!() |> sha256()}
  rescue
    _error -> {:error, {:invalid_runner_release_field, :identity_payload, :encode_failed}}
  end

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)

  defp schema_version(attrs) do
    case Validation.fetch(attrs, :schema_version) do
      {:ok, @schema_version} ->
        {:ok, @schema_version}

      {:ok, value} ->
        {:error, {:unsupported_runner_release_schema, value, @schema_version}}

      {:error, _reason} = error ->
        error
    end
  end

  defp favn_version(attrs) do
    with {:ok, value} <- required_string(attrs, :favn_version, 128),
         {:ok, parsed} <- parse_version(value, :favn_version),
         {:ok, current} <- Version.parse(current_favn_version()),
         true <- parsed.major == current.major and parsed.minor == current.minor do
      {:ok, value}
    else
      false ->
        {:error,
         {:unsupported_favn_version, Validation.fetch_optional(attrs, :favn_version),
          favn_series()}}

      {:error, {:invalid_runner_release_field, _field, _reason}} = error ->
        error

      _error ->
        {:error,
         {:unsupported_favn_version, Validation.fetch_optional(attrs, :favn_version),
          favn_series()}}
    end
  end

  defp runner_contract_version(attrs) do
    current = Compatibility.current_runner_contract_version()

    with {:ok, value} <- required_positive_integer(attrs, :runner_contract_version),
         true <- value == current do
      {:ok, value}
    else
      false ->
        {:error,
         {:unsupported_runner_contract,
          Validation.fetch_optional(attrs, :runner_contract_version), current}}

      {:error, _reason} = error ->
        error
    end
  end

  defp semantic_version(attrs, field) do
    with {:ok, value} <- required_string(attrs, field, 128),
         {:ok, _parsed} <- parse_version(value, field) do
      {:ok, value}
    end
  end

  defp parse_version(value, field) do
    case Version.parse(value) do
      {:ok, parsed} -> {:ok, parsed}
      :error -> {:error, {:invalid_runner_release_field, field, :invalid_version}}
    end
  end

  defp otp_release(attrs) do
    with {:ok, value} <- required_string(attrs, :otp_release, 32),
         true <- Regex.match?(~r/\A[0-9]+\z/, value) do
      {:ok, value}
    else
      false -> {:error, {:invalid_runner_release_field, :otp_release, :invalid_version}}
      {:error, _reason} = error -> error
    end
  end

  defp exact_string(attrs, field, expected) do
    case Validation.fetch(attrs, field) do
      {:ok, ^expected} -> {:ok, expected}
      {:ok, _value} -> {:error, {:invalid_runner_release_field, field, :unsupported_value}}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_entries(attrs, field, builder, identity_field) do
    case Validation.fetch(attrs, field) do
      {:ok, values} -> normalize_existing_entries(values, builder, identity_field)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_existing_entries(values, builder, identity_field) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case builder.(value) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> sort_and_reject_duplicates(normalized, identity_field)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_existing_entries(_values, _builder, field),
    do: {:error, {:invalid_runner_release_field, field, :expected_list}}

  defp sort_and_reject_duplicates(entries, field) do
    entries = Enum.sort_by(entries, &Map.fetch!(&1, field))

    case Enum.chunk_by(entries, &Map.fetch!(&1, field)) |> Enum.find(&(length(&1) > 1)) do
      nil ->
        {:ok, entries}

      [duplicate | _rest] ->
        {:error, {:duplicate_runner_release_entry, field, Map.fetch!(duplicate, field)}}
    end
  end

  defp match_optional_digest(attrs, field, expected) do
    case Validation.fetch_optional(attrs, field) do
      nil ->
        :ok

      supplied ->
        case Validation.digest(supplied, field) do
          {:ok, ^expected} -> :ok
          {:ok, _other} -> digest_mismatch(field, expected, supplied)
          {:error, _reason} -> digest_mismatch(field, expected, supplied)
        end
    end
  end

  defp digest_mismatch(:runtime_code_digest, expected, actual),
    do: {:error, {:runtime_code_digest_mismatch, expected, actual}}

  defp digest_mismatch(:runtime_dependency_digest, expected, actual),
    do: {:error, {:runtime_dependency_digest_mismatch, expected, actual}}

  defp match_optional_id(attrs, expected) do
    case Validation.fetch_optional(attrs, :runner_release_id) do
      nil -> :ok
      ^expected -> :ok
      actual -> {:error, {:runner_release_id_mismatch, expected, actual}}
    end
  end

  defp validate_id_syntax(value) when is_binary(value) do
    if Regex.match?(@runner_release_id, value) do
      :ok
    else
      {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}}
    end
  end

  defp validate_id_syntax(_value),
    do: {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}}

  defp require_descriptor_fields(value) do
    Enum.reduce_while(@descriptor_fields, :ok, fn field, :ok ->
      case Validation.fetch(value, field) do
        {:ok, _value} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp build_metadata(attrs) do
    value = Validation.fetch_optional(attrs, :build_metadata, %{})

    cond do
      not is_map(value) ->
        {:error, {:invalid_runner_release_field, :build_metadata, :expected_map}}

      not json_value?(value) ->
        {:error, {:invalid_runner_release_field, :build_metadata, :invalid_json_value}}

      true ->
        case Serializer.encode_canonical(value) do
          {:ok, encoded} when byte_size(encoded) <= @max_build_metadata_bytes ->
            {:ok, value}

          {:ok, _encoded} ->
            {:error, {:invalid_runner_release_field, :build_metadata, :too_large}}

          {:error, _reason} ->
            {:error, {:invalid_runner_release_field, :build_metadata, :invalid_json_value}}
        end
    end
  end

  defp json_value?(value) when is_binary(value) or is_number(value) or is_boolean(value), do: true
  defp json_value?(nil), do: true
  defp json_value?(value) when is_atom(value), do: true
  defp json_value?(values) when is_list(values), do: Enum.all?(values, &json_value?/1)

  defp json_value?(value) when is_map(value) and not is_struct(value) do
    Enum.all?(value, fn {key, child} ->
      (is_binary(key) or is_atom(key)) and json_value?(child)
    end)
  end

  defp json_value?(_value), do: false

  defp required_string(attrs, field, max_bytes) do
    case Validation.fetch(attrs, field) do
      {:ok, value} -> Validation.string(value, field, max_bytes)
      {:error, _reason} = error -> error
    end
  end

  defp required_positive_integer(attrs, field) do
    case Validation.fetch(attrs, field) do
      {:ok, value} -> Validation.positive_integer(value, field)
      {:error, _reason} = error -> error
    end
  end

  defp favn_series do
    case Version.parse(current_favn_version()) do
      {:ok, version} -> "#{version.major}.#{version.minor}.x"
      :error -> current_favn_version()
    end
  end
end
