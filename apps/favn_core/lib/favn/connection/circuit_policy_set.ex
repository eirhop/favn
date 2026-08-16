defmodule Favn.Connection.CircuitPolicySet do
  @moduledoc """
  Validated, non-secret circuit policies keyed by connection name.

  `from_connection_config/1` deliberately extracts only Favn-owned circuit
  policy. Adapter options, runtime references, and secrets never enter this
  value or the manifest.
  """

  alias Favn.CircuitBreaker.Policy
  alias Favn.Manifest.Serializer

  @max_connections 450
  @connection_name ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\z/

  @type t :: %{optional(String.t()) => Policy.t()}

  @doc "Extracts enabled circuit policies from consumer connection configuration."
  @spec from_connection_config(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def from_connection_config(entries) do
    with {:ok, entries} <- normalize_connection_entries(entries) do
      Enum.reduce_while(entries, {:ok, %{}}, fn {name, config}, {:ok, policies} ->
        with {:ok, config} <- normalize_connection_config(name, config),
             {:ok, policy} <- circuit_policy(config) do
          case policy do
            nil ->
              {:cont, {:ok, policies}}

            %Policy{} when map_size(policies) < @max_connections ->
              {:cont, {:ok, Map.put(policies, name, policy)}}

            %Policy{} ->
              {:halt,
               {:error,
                {:too_many_connection_circuit_policies, @max_connections + 1, @max_connections}}}
          end
        else
          {:error, reason} ->
            {:halt,
             {:error, {:invalid_connection_circuit_policy, name, safe_policy_reason(reason)}}}
        end
      end)
    end
  end

  @doc "Validates a persisted connection-name to circuit-policy map."
  @spec new(keyword() | map() | t()) :: {:ok, t()} | {:error, term()}
  def new(entries) do
    with {:ok, entries} <- normalize_entries(entries) do
      Enum.reduce_while(entries, {:ok, %{}}, fn {name, value}, {:ok, policies} ->
        case Policy.new(value) do
          {:ok, %Policy{} = policy} ->
            {:cont, {:ok, Map.put(policies, name, policy)}}

          {:ok, nil} ->
            {:halt, {:error, {:connection_circuit_policy_required, name}}}

          {:error, reason} ->
            {:halt,
             {:error, {:invalid_connection_circuit_policy, name, safe_policy_reason(reason)}}}
        end
      end)
    end
  end

  @doc "Returns the largest accepted connection circuit-policy catalogue."
  @spec maximum_connections() :: pos_integer()
  def maximum_connections, do: @max_connections

  @doc "Keeps policies for the compiled connection catalogue only."
  @spec select(t(), [atom() | String.t()]) :: t()
  def select(policies, names) when is_map(policies) and is_list(names) do
    names = MapSet.new(names, &to_string/1)
    Map.take(policies, MapSet.to_list(names))
  end

  @doc "Returns the stable JSON-compatible policy map."
  @spec to_map(t()) :: map()
  def to_map(policies) when is_map(policies) do
    Map.new(policies, fn {name, %Policy{} = policy} ->
      {name,
       %{
         "failure_threshold" => policy.failure_threshold,
         "probe_after_ms" => policy.probe_after_ms
       }}
    end)
  end

  @doc "Returns a stable fingerprint for deployment integrity checks."
  @spec fingerprint(t()) :: String.t()
  def fingerprint(policies) when is_map(policies) do
    policies
    |> to_map()
    |> Serializer.encode_canonical!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp normalize_entries(entries) when is_list(entries) do
    if Keyword.keyword?(entries),
      do: normalize_entries_map(entries),
      else: {:error, :invalid_connection_configuration}
  end

  defp normalize_entries(entries) when is_map(entries),
    do: normalize_entries_map(Map.to_list(entries))

  defp normalize_entries(_entries), do: {:error, :invalid_connection_configuration}

  defp normalize_connection_entries(entries) when is_list(entries) do
    if Keyword.keyword?(entries),
      do: normalize_entries_map_unbounded(entries),
      else: {:error, :invalid_connection_configuration}
  end

  defp normalize_connection_entries(entries) when is_map(entries),
    do: normalize_entries_map_unbounded(Map.to_list(entries))

  defp normalize_connection_entries(_entries), do: {:error, :invalid_connection_configuration}

  defp normalize_entries_map(entries) when length(entries) <= @max_connections do
    normalize_entries_map_unbounded(entries)
  end

  defp normalize_entries_map(entries),
    do: {:error, {:too_many_connection_circuit_policies, length(entries), @max_connections}}

  defp normalize_entries_map_unbounded(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn
      {name, value}, {:ok, normalized} when is_atom(name) or is_binary(name) ->
        name = to_string(name)

        cond do
          not Regex.match?(@connection_name, name) ->
            {:halt, {:error, {:invalid_connection_name, name}}}

          Map.has_key?(normalized, name) ->
            {:halt, {:error, {:duplicate_connection_name, name}}}

          true ->
            {:cont, {:ok, Map.put(normalized, name, value)}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_connection_configuration_entry}}
    end)
  end

  defp normalize_connection_config(_name, config) when is_map(config), do: {:ok, config}

  defp normalize_connection_config(_name, config) when is_list(config) do
    if Keyword.keyword?(config),
      do: {:ok, config},
      else: {:error, :invalid_connection_configuration}
  end

  defp normalize_connection_config(_name, _config),
    do: {:error, :invalid_connection_configuration}

  defp circuit_policy(config) when is_list(config) do
    case Keyword.get_values(config, :circuit_breaker) do
      [] -> Policy.new(nil)
      [value] -> Policy.new(value)
      _values -> {:error, {:duplicate_connection_options, ["circuit_breaker"]}}
    end
  end

  defp circuit_policy(config) when is_map(config) do
    atom? = Map.has_key?(config, :circuit_breaker)
    string? = Map.has_key?(config, "circuit_breaker")

    if atom? and string? do
      {:error, {:duplicate_connection_options, ["circuit_breaker"]}}
    else
      Policy.new(Map.get(config, :circuit_breaker, Map.get(config, "circuit_breaker")))
    end
  end

  defp safe_policy_reason({:invalid_circuit_breaker_failure_threshold, _value}),
    do: :invalid_circuit_breaker_failure_threshold

  defp safe_policy_reason({:invalid_circuit_breaker_probe_after_ms, _value}),
    do: :invalid_circuit_breaker_probe_after_ms

  defp safe_policy_reason({:duplicate_connection_options, keys}),
    do: {:duplicate_connection_options, keys}

  defp safe_policy_reason({:duplicate_circuit_breaker_options, keys}),
    do: {:duplicate_circuit_breaker_options, keys}

  defp safe_policy_reason(_reason), do: :invalid_circuit_breaker_policy
end
