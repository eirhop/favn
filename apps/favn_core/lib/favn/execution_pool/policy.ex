defmodule Favn.ExecutionPool.Policy do
  @moduledoc """
  Validated concurrency and circuit-breaker defaults for one execution pool.

  Execution-pool policy is immutable manifest data. The orchestrator may apply
  a persisted operator override when it activates that manifest for a workspace.
  """

  alias Favn.CircuitBreaker.Policy, as: CircuitBreakerPolicy

  @max_concurrency 1_000_000
  @allowed_keys [:max_concurrency, :circuit_breaker, "max_concurrency", "circuit_breaker"]

  @enforce_keys [:max_concurrency]
  defstruct [:max_concurrency, :circuit_breaker]

  @type t :: %__MODULE__{
          max_concurrency: pos_integer(),
          circuit_breaker: CircuitBreakerPolicy.t() | nil
        }

  @doc "Builds a strictly validated execution-pool policy."
  @spec new(term()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = policy), do: validate(policy)

  def new(value) when is_list(value) do
    if Keyword.keyword?(value) do
      with :ok <- reject_duplicate_keys(Keyword.keys(value)) do
        new(Map.new(value))
      end
    else
      {:error, {:invalid_execution_pool_policy, value}}
    end
  end

  def new(value) when is_map(value) do
    with :ok <- reject_unknown_keys(value),
         {:ok, circuit_breaker} <-
           value |> field(:circuit_breaker) |> CircuitBreakerPolicy.new() do
      validate(%__MODULE__{
        max_concurrency: field(value, :max_concurrency),
        circuit_breaker: circuit_breaker
      })
    end
  end

  def new(value), do: {:error, {:invalid_execution_pool_policy, value}}

  @doc "Validates an already normalized execution-pool policy."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = policy) do
    with true <- valid_concurrency?(policy.max_concurrency),
         {:ok, circuit_breaker} <- CircuitBreakerPolicy.new(policy.circuit_breaker) do
      {:ok, %{policy | circuit_breaker: circuit_breaker}}
    else
      false -> {:error, {:invalid_execution_pool_max_concurrency, policy.max_concurrency}}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns a stable JSON-facing policy map."
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = policy) do
    %{
      "max_concurrency" => policy.max_concurrency,
      "circuit_breaker" => circuit_breaker_map(policy.circuit_breaker)
    }
  end

  @doc "Returns the largest accepted execution-pool concurrency limit."
  @spec max_concurrency() :: pos_integer()
  def max_concurrency, do: @max_concurrency

  defp reject_unknown_keys(value) do
    unknown = value |> Map.keys() |> Enum.reject(&(&1 in @allowed_keys))

    with true <- unknown == [],
         :ok <- reject_duplicate_keys(Map.keys(value)) do
      :ok
    else
      false ->
        {:error, {:unknown_execution_pool_policy_keys, Enum.sort_by(unknown, &inspect/1)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp reject_duplicate_keys(keys) do
    normalized = Enum.map(keys, &to_string/1)
    duplicates = normalized -- Enum.uniq(normalized)

    if duplicates == [],
      do: :ok,
      else: {:error, {:duplicate_execution_pool_policy_keys, Enum.sort(Enum.uniq(duplicates))}}
  end

  defp valid_concurrency?(value),
    do: is_integer(value) and value > 0 and value <= @max_concurrency

  defp circuit_breaker_map(nil), do: nil

  defp circuit_breaker_map(%CircuitBreakerPolicy{} = policy) do
    %{
      "failure_threshold" => policy.failure_threshold,
      "probe_after_ms" => policy.probe_after_ms
    }
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
