defmodule Favn.Manifest.Compatibility do
  @moduledoc """
  Manifest schema and runner contract compatibility checks.
  """

  @current_schema_version 8
  @current_runner_contract_version 8

  @type error ::
          {:invalid_manifest_input, term()}
          | {:missing_manifest_field, :schema_version | :runner_contract_version}
          | {:invalid_execution_package_hash, Favn.Ref.t(), term()}
          | {:duplicate_execution_package_hash, String.t(), [Favn.Ref.t()]}
          | {:missing_execution_package_hash, Favn.Ref.t()}
          | {:unexpected_execution_package_hash, Favn.Ref.t()}
          | {:unsupported_schema_version, term(), pos_integer()}
          | {:unsupported_runner_contract_version, term(), pos_integer()}

  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @current_schema_version

  @spec current_runner_contract_version() :: pos_integer()
  def current_runner_contract_version, do: @current_runner_contract_version

  @spec validate_manifest(term()) :: :ok | {:error, error()}
  def validate_manifest(manifest) when is_map(manifest) or is_struct(manifest) do
    with {:ok, schema_version} <- read_required_field(manifest, :schema_version),
         {:ok, runner_contract_version} <-
           read_required_field(manifest, :runner_contract_version),
         :ok <- validate_schema_version(schema_version),
         :ok <- validate_runner_contract_version(runner_contract_version) do
      validate_execution_package_refs(manifest)
    end
  end

  def validate_manifest(other), do: {:error, {:invalid_manifest_input, other}}

  @spec validate_schema_version(term()) :: :ok | {:error, error()}
  def validate_schema_version(@current_schema_version), do: :ok

  def validate_schema_version(other),
    do: {:error, {:unsupported_schema_version, other, @current_schema_version}}

  @spec validate_runner_contract_version(term()) :: :ok | {:error, error()}
  def validate_runner_contract_version(@current_runner_contract_version), do: :ok

  def validate_runner_contract_version(other),
    do: {:error, {:unsupported_runner_contract_version, other, @current_runner_contract_version}}

  defp validate_execution_package_refs(manifest) do
    assets = Map.get(manifest, :assets, Map.get(manifest, "assets", []))

    with :ok <- validate_asset_package_refs(assets) do
      validate_unique_package_hashes(assets)
    end
  end

  defp validate_asset_package_refs(assets) do
    Enum.reduce_while(assets, :ok, fn asset, :ok ->
      type = Map.get(asset, :type, Map.get(asset, "type"))
      ref = Map.get(asset, :ref, Map.get(asset, "ref"))
      hash = Map.get(asset, :execution_package_hash, Map.get(asset, "execution_package_hash"))

      case {type, hash} do
        {:sql, value} when is_binary(value) ->
          if canonical_hash?(value) do
            {:cont, :ok}
          else
            {:halt, {:error, {:invalid_execution_package_hash, ref, value}}}
          end

        {:sql, nil} ->
          {:halt, {:error, {:missing_execution_package_hash, ref}}}

        {:sql, value} ->
          {:halt, {:error, {:invalid_execution_package_hash, ref, value}}}

        {_type, nil} ->
          {:cont, :ok}

        {_type, _value} ->
          {:halt, {:error, {:unexpected_execution_package_hash, ref}}}
      end
    end)
  end

  defp validate_unique_package_hashes(assets) do
    assets
    |> Enum.flat_map(fn asset ->
      case {
        Map.get(asset, :execution_package_hash, Map.get(asset, "execution_package_hash")),
        Map.get(asset, :ref, Map.get(asset, "ref"))
      } do
        {hash, ref} when is_binary(hash) -> [{hash, ref}]
        _other -> []
      end
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.find(fn {_hash, refs} -> length(refs) > 1 end)
    |> case do
      nil -> :ok
      {hash, refs} -> {:error, {:duplicate_execution_package_hash, hash, Enum.sort(refs)}}
    end
  end

  defp canonical_hash?(hash), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)

  defp read_required_field(value, field) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> {:ok, Map.get(value, atom_key)}
      Map.has_key?(value, string_key) -> {:ok, Map.get(value, string_key)}
      true -> {:error, {:missing_manifest_field, field}}
    end
  end
end
