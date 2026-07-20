defmodule FavnOrchestrator.ResourceConfiguration do
  @moduledoc """
  Normalizes Favn-owned resource policies from boot-time application config.

  Circuit-breaker configuration is colocated with execution pools and
  connections, but it remains an orchestrator policy and never becomes an
  arbitrary adapter option.
  """

  alias Favn.CircuitBreaker.Policy
  alias Favn.Resource.Ref

  @doc "Returns the configured circuit-breaker policy for a resource."
  @spec circuit_breaker(Ref.t()) :: {:ok, Policy.t() | nil} | {:error, term()}
  def circuit_breaker(%Ref{kind: :execution_pool, name: name}),
    do: configured_policy(:execution_pools, name)

  def circuit_breaker(%Ref{kind: :connection, name: name}),
    do: configured_policy(:connections, name)

  @doc "Returns a resource only when it has an enabled circuit breaker."
  @spec enabled_resource(Ref.t()) :: {:ok, {Ref.t(), Policy.t()} | nil} | {:error, term()}
  def enabled_resource(%Ref{} = ref) do
    case circuit_breaker(ref) do
      {:ok, %Policy{} = policy} -> {:ok, {ref, policy}}
      {:ok, nil} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp configured_policy(root, name) do
    with {:ok, resources} <- resource_entries(root),
         {:ok, config} <- fetch_resource(resources, name) do
      config
      |> field(:circuit_breaker)
      |> Policy.new()
      |> case do
        {:ok, policy} -> {:ok, policy}
        {:error, reason} -> {:error, {:invalid_resource_circuit_breaker, root, name, reason}}
      end
    else
      :error -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resource_entries(root) do
    case Application.get_env(:favn, root, []) do
      entries when is_list(entries) or is_map(entries) -> {:ok, entries}
      value -> {:error, {:invalid_resource_configuration, root, value}}
    end
  end

  defp fetch_resource(entries, name) do
    normalized_name = to_string(name)

    Enum.find_value(entries, :error, fn
      {entry_name, value} when is_atom(entry_name) or is_binary(entry_name) ->
        if to_string(entry_name) == normalized_name, do: {:ok, value}, else: false

      _invalid ->
        false
    end)
  end

  defp field(value, key) when is_list(value) do
    if Keyword.keyword?(value), do: Keyword.get(value, key), else: nil
  end

  defp field(value, key) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key)))

  defp field(_value, _key), do: nil
end
