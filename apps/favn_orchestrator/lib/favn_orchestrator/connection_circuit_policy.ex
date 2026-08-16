defmodule FavnOrchestrator.ConnectionCircuitPolicy do
  @moduledoc """
  Immutable connection circuit policy frozen with one workspace deployment.

  Only Favn-owned, non-secret policy crosses this boundary. Adapter connection
  values and runtime references remain runner-local.
  """

  alias Favn.Connection.CircuitPolicySet
  alias Favn.Manifest

  @configuration_key "connection_circuit_policy"

  @doc "Stores the manifest policy and integrity fingerprint in deployment configuration."
  @spec put(map(), Manifest.t()) :: {:ok, map()} | {:error, term()}
  def put(configuration, %Manifest{connection_circuits: connection_circuits})
      when is_map(configuration) do
    with {:ok, policies} <- CircuitPolicySet.new(connection_circuits) do
      {:ok,
       configuration
       |> Map.delete(:schema_version)
       |> Map.delete(:connection_circuit_policy)
       |> Map.put("schema_version", 3)
       |> Map.put(@configuration_key, %{
         "schema_version" => 1,
         "fingerprint" => CircuitPolicySet.fingerprint(policies),
         "effective" => CircuitPolicySet.to_map(policies)
       })}
    end
  end

  @doc "Returns the validated effective policy from immutable deployment configuration."
  @spec effective(map()) :: {:ok, CircuitPolicySet.t()} | {:error, term()}
  def effective(configuration) when is_map(configuration) do
    case field(configuration, @configuration_key) do
      nil -> legacy_effective(configuration)
      %{} = block -> effective_block(block)
      _invalid -> {:error, :invalid_connection_circuit_policy}
    end
  end

  def effective(_configuration), do: {:error, :invalid_connection_circuit_policy}

  defp effective_block(block) do
    with :ok <- validate_keys(block),
         1 <- field(block, "schema_version"),
         fingerprint when is_binary(fingerprint) <- field(block, "fingerprint"),
         %{} = effective <- field(block, "effective"),
         {:ok, policies} <- CircuitPolicySet.new(effective),
         true <- fingerprint == CircuitPolicySet.fingerprint(policies) do
      {:ok, policies}
    else
      version when is_integer(version) ->
        {:error, {:unsupported_connection_circuit_policy_version, version}}

      false ->
        {:error, :connection_circuit_policy_fingerprint_mismatch}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_connection_circuit_policy}
    end
  end

  defp legacy_effective(configuration) do
    case field(configuration, "schema_version") || 1 do
      version when version in [1, 2] -> {:ok, %{}}
      _version -> {:error, :invalid_connection_circuit_policy}
    end
  end

  @doc "Returns bounded, non-secret policy diagnostics."
  @spec diagnostics(map()) :: {:ok, [map()]} | {:error, term()}
  def diagnostics(configuration) when is_map(configuration) do
    with {:ok, policies} <- effective(configuration) do
      {:ok,
       policies
       |> Enum.map(fn {name, policy} ->
         %{
           name: name,
           failure_threshold: policy.failure_threshold,
           probe_after_ms: policy.probe_after_ms,
           source: "manifest"
         }
       end)
       |> Enum.sort_by(& &1.name)}
    end
  end

  @doc "Returns the deployment configuration key for storage validation."
  @spec configuration_key() :: String.t()
  def configuration_key, do: @configuration_key

  defp field(map, key) when is_binary(key) do
    atom_key = safe_existing_atom(key)

    cond do
      Map.has_key?(map, key) -> Map.get(map, key)
      not is_nil(atom_key) and Map.has_key?(map, atom_key) -> Map.get(map, atom_key)
      true -> nil
    end
  end

  defp safe_existing_atom(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp validate_keys(block) do
    allowed = ~w(schema_version fingerprint effective)

    block
    |> Map.keys()
    |> Enum.reduce_while({:ok, MapSet.new()}, fn
      key, {:ok, seen} when is_atom(key) or is_binary(key) ->
        normalized = to_string(key)

        cond do
          normalized not in allowed ->
            {:halt, {:error, {:unknown_connection_circuit_policy_key, normalized}}}

          MapSet.member?(seen, normalized) ->
            {:halt, {:error, :duplicate_connection_circuit_policy_keys}}

          true ->
            {:cont, {:ok, MapSet.put(seen, normalized)}}
        end

      key, _acc ->
        {:halt, {:error, {:invalid_connection_circuit_policy_key, key}}}
    end)
    |> case do
      {:ok, _seen} -> :ok
      {:error, _reason} = error -> error
    end
  end
end
