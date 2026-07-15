defmodule Favn.SQL.SessionRequirements do
  @moduledoc """
  Manifest-safe requirements for preparing a SQL asset session.

  The manifest stores stable resource names only. Runtime connection
  configuration owns the SQL files and runtime values behind those names.
  Adapters resolve the names while preparing a physical session.

  Resource names are normalized to strings so persisted manifests never create
  atoms from runtime input. A SQL asset may require at most 128 resources, and
  each name is limited to 128 bytes.
  """

  @current_version 1
  @max_resources 128
  @max_name_bytes 128
  @name_pattern ~r/^[a-z][a-z0-9_]*$/
  @contract_keys [:version, :resources]

  @enforce_keys [:version]
  defstruct version: @current_version, resources: []

  @type t :: %__MODULE__{version: pos_integer(), resources: [String.t()]}

  @doc """
  Returns an empty requirements contract using the current version.
  """
  @spec empty() :: t()
  def empty, do: %__MODULE__{version: @current_version}

  @doc """
  Builds and validates requirements from a resource-name list.

  Names may be authored as atoms or strings. They are normalized, deduplicated,
  and sorted for deterministic manifests and session-pool identity.

  ## Examples

      iex> Favn.SQL.SessionRequirements.new!([:landing_storage, "vendor_api"])
      %Favn.SQL.SessionRequirements{version: 1, resources: ["landing_storage", "vendor_api"]}
  """
  @spec new!([atom() | String.t()]) :: t()
  def new!(resources) when is_list(resources) do
    %__MODULE__{version: @current_version, resources: normalize_resources!(resources)}
  end

  def new!(resources) do
    raise ArgumentError,
          "SQL session resources must be a list of atom or string names, got: #{inspect(resources)}"
  end

  @doc """
  Validates and canonicalizes an existing requirements value.
  """
  @spec validate!(t() | map()) :: t()
  def validate!(%__MODULE__{version: @current_version, resources: resources}), do: new!(resources)

  def validate!(%__MODULE__{version: version}) do
    raise ArgumentError,
          "unsupported SQL session requirements version #{inspect(version)}; expected #{@current_version}"
  end

  def validate!(value) when is_map(value) do
    unknown = Enum.reject(Map.keys(value), &known_contract_key?/1)
    duplicates = duplicate_contract_keys(value)

    cond do
      unknown != [] ->
        raise ArgumentError,
              "unknown SQL session requirements fields: #{inspect(Enum.sort(unknown))}"

      duplicates != [] ->
        raise ArgumentError,
              "duplicate SQL session requirements fields: #{inspect(duplicates)}"

      true ->
        version = Map.get(value, :version, Map.get(value, "version"))
        resources = Map.get(value, :resources, Map.get(value, "resources", []))

        validate!(%__MODULE__{version: version, resources: resources})
    end
  end

  def validate!(value) do
    raise ArgumentError, "invalid SQL session requirements: #{inspect(value)}"
  end

  @doc false
  @spec normalize_resources!([atom() | String.t()]) :: [String.t()]
  def normalize_resources!(resources) when is_list(resources) do
    normalized = resources |> Enum.map(&normalize_name!/1) |> Enum.uniq() |> Enum.sort()

    if length(normalized) > @max_resources do
      raise ArgumentError,
            "SQL session resources exceed the #{@max_resources}-resource limit"
    end

    normalized
  end

  defp normalize_name!(name) when is_atom(name) and not is_nil(name),
    do: name |> Atom.to_string() |> normalize_name!()

  defp normalize_name!(name) when is_binary(name) do
    cond do
      name == "" ->
        raise ArgumentError, "SQL session resource names cannot be empty"

      byte_size(name) > @max_name_bytes ->
        raise ArgumentError,
              "SQL session resource name exceeds #{@max_name_bytes} bytes: #{inspect(name)}"

      not Regex.match?(@name_pattern, name) ->
        raise ArgumentError,
              "invalid SQL session resource name #{inspect(name)}; expected lowercase snake_case"

      true ->
        name
    end
  end

  defp normalize_name!(name) do
    raise ArgumentError,
          "SQL session resource names must be atoms or strings, got: #{inspect(name)}"
  end

  defp known_contract_key?(key) when is_atom(key), do: key in @contract_keys

  defp known_contract_key?(key) when is_binary(key),
    do: Enum.any?(@contract_keys, &(Atom.to_string(&1) == key))

  defp known_contract_key?(_key), do: false

  defp duplicate_contract_keys(value) do
    @contract_keys
    |> Enum.filter(fn key ->
      Map.has_key?(value, key) and Map.has_key?(value, Atom.to_string(key))
    end)
    |> Enum.map(&Atom.to_string/1)
  end
end
