defmodule Favn.Manifest.Compatibility do
  @moduledoc """
  Manifest schema and runner contract compatibility checks.
  """

  @current_schema_version 1
  @current_runner_contract_version 1

  @type error ::
          {:unsupported_schema_version, term(), pos_integer()}
          | {:unsupported_runner_contract_version, term(), pos_integer()}

  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @current_schema_version

  @spec current_runner_contract_version() :: pos_integer()
  def current_runner_contract_version, do: @current_runner_contract_version

  @spec validate_manifest(map() | struct()) :: :ok | {:error, error()}
  def validate_manifest(manifest) when is_map(manifest) or is_struct(manifest) do
    schema_version = read_field(manifest, :schema_version, @current_schema_version)

    runner_contract_version =
      read_field(manifest, :runner_contract_version, @current_runner_contract_version)

    with :ok <- validate_schema_version(schema_version) do
      validate_runner_contract_version(runner_contract_version)
    end
  end

  @spec validate_schema_version(term()) :: :ok | {:error, error()}
  def validate_schema_version(@current_schema_version), do: :ok

  def validate_schema_version(other),
    do: {:error, {:unsupported_schema_version, other, @current_schema_version}}

  @spec validate_runner_contract_version(term()) :: :ok | {:error, error()}
  def validate_runner_contract_version(@current_runner_contract_version), do: :ok

  def validate_runner_contract_version(other),
    do: {:error, {:unsupported_runner_contract_version, other, @current_runner_contract_version}}

  defp read_field(value, field, default) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> Map.get(value, atom_key)
      Map.has_key?(value, string_key) -> Map.get(value, string_key)
      true -> default
    end
  end
end
