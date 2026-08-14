defmodule Favn.ExecutionPool.PolicySet do
  @moduledoc """
  Bounded, normalized execution-pool policy catalogue.

  Source names may be atoms or strings. Runtime names are stable strings and
  are never converted into new atoms.
  """

  alias Favn.ExecutionPool.Policy
  alias Favn.Manifest.Serializer

  @maximum_pools 400
  @pool_name ~r/\A[A-Za-z0-9][A-Za-z0-9_.-]{0,62}\z/

  @type t :: %{optional(String.t()) => Policy.t()}

  @doc "Normalizes and strictly validates a complete execution-pool catalogue."
  @spec new(term()) :: {:ok, t()} | {:error, term()}
  def new(nil), do: {:error, {:invalid_execution_pool_configuration, nil}}

  def new(entries) when is_list(entries) do
    if Keyword.keyword?(entries) do
      normalize_entries(entries)
    else
      {:error, {:invalid_execution_pool_configuration, entries}}
    end
  end

  def new(entries) when is_map(entries), do: normalize_entries(Map.to_list(entries))
  def new(entries), do: {:error, {:invalid_execution_pool_configuration, entries}}

  @doc "Returns the largest accepted execution-pool catalogue."
  @spec maximum_pools() :: pos_integer()
  def maximum_pools, do: @maximum_pools

  @doc "Returns a stable JSON-facing catalogue map."
  @spec to_map(t()) :: map()
  def to_map(policies) when is_map(policies) do
    Map.new(policies, fn {name, %Policy{} = policy} -> {name, Policy.to_map(policy)} end)
  end

  @doc "Returns bounded non-sensitive policy diagnostics."
  @spec diagnostics(t()) :: [map()]
  def diagnostics(policies) when is_map(policies) do
    policies
    |> Enum.map(fn {name, %Policy{} = policy} ->
      %{
        name: name,
        max_concurrency: policy.max_concurrency,
        circuit_breaker:
          case policy.circuit_breaker do
            nil ->
              nil

            breaker ->
              %{
                failure_threshold: breaker.failure_threshold,
                probe_after_ms: breaker.probe_after_ms
              }
          end
      }
    end)
    |> Enum.sort_by(& &1.name)
  end

  @doc "Returns the canonical SHA-256 fingerprint for a normalized catalogue."
  @spec fingerprint(t()) :: String.t()
  def fingerprint(policies) when is_map(policies) do
    policies
    |> to_map()
    |> Serializer.encode_canonical!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp normalize_entries(entries) when length(entries) <= @maximum_pools do
    entries
    |> Enum.reduce_while({:ok, %{}}, fn
      {name, value}, {:ok, acc} when is_atom(name) or is_binary(name) ->
        normalized_name = to_string(name)

        cond do
          not Regex.match?(@pool_name, normalized_name) ->
            {:halt, {:error, {:invalid_execution_pool_name, normalized_name}}}

          Map.has_key?(acc, normalized_name) ->
            {:halt, {:error, {:duplicate_execution_pool_name, normalized_name}}}

          true ->
            case Policy.new(value) do
              {:ok, policy} ->
                {:cont, {:ok, Map.put(acc, normalized_name, policy)}}

              {:error, reason} ->
                {:halt, {:error, {:invalid_execution_pool_policy, normalized_name, reason}}}
            end
        end

      invalid, {:ok, _acc} ->
        {:halt, {:error, {:invalid_execution_pool_entry, invalid}}}
    end)
  end

  defp normalize_entries(entries),
    do: {:error, {:too_many_execution_pools, length(entries), @maximum_pools}}
end
