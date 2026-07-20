defmodule Favn.CircuitBreaker.Policy do
  @moduledoc """
  Shared runtime policy for one Favn-owned resource circuit breaker.

  A circuit opens after `failure_threshold` consecutive terminal resource
  failures. Once `probe_after_ms` has elapsed, one normal node may probe the
  resource. A successful probe closes the circuit; a failed probe reopens it.

  This policy is separate from `Favn.Retry.Policy`: retries decide whether one
  node may repeat, while a circuit breaker decides whether new work may start.
  """

  @max_failure_threshold 10_000
  @max_probe_after_ms 86_400_000

  @enforce_keys [:failure_threshold, :probe_after_ms]
  defstruct [:failure_threshold, :probe_after_ms]

  @type t :: %__MODULE__{
          failure_threshold: pos_integer(),
          probe_after_ms: pos_integer()
        }

  @doc "Builds and validates a policy from a map or keyword list."
  @spec new(term()) :: {:ok, t() | nil} | {:error, term()}
  def new(nil), do: {:ok, nil}
  def new(%__MODULE__{} = policy), do: validate(policy)

  def new(value) when is_list(value) do
    if Keyword.keyword?(value),
      do: new(Map.new(value)),
      else: {:error, {:invalid_circuit_breaker_policy, value}}
  end

  def new(value) when is_map(value) do
    with :ok <- reject_unknown_keys(value) do
      validate(%__MODULE__{
        failure_threshold: field(value, :failure_threshold),
        probe_after_ms: field(value, :probe_after_ms)
      })
    end
  end

  def new(value), do: {:error, {:invalid_circuit_breaker_policy, value}}

  @doc "Builds and validates a policy or raises `ArgumentError`."
  @spec new!(term()) :: t() | nil
  def new!(value) do
    case new(value) do
      {:ok, policy} ->
        policy

      {:error, reason} ->
        raise ArgumentError, "invalid circuit breaker policy: #{inspect(reason)}"
    end
  end

  @doc "Validates an already-normalized policy."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = policy) do
    cond do
      not valid_threshold?(policy.failure_threshold) ->
        {:error, {:invalid_circuit_breaker_failure_threshold, policy.failure_threshold}}

      not valid_probe_after?(policy.probe_after_ms) ->
        {:error, {:invalid_circuit_breaker_probe_after_ms, policy.probe_after_ms}}

      true ->
        {:ok, policy}
    end
  end

  @doc "Returns the largest accepted consecutive-failure threshold."
  @spec max_failure_threshold() :: pos_integer()
  def max_failure_threshold, do: @max_failure_threshold

  @doc "Returns the largest accepted probe delay."
  @spec max_probe_after_ms() :: pos_integer()
  def max_probe_after_ms, do: @max_probe_after_ms

  defp reject_unknown_keys(value) do
    allowed =
      MapSet.new([:failure_threshold, :probe_after_ms, "failure_threshold", "probe_after_ms"])

    unknown = value |> Map.keys() |> Enum.reject(&MapSet.member?(allowed, &1))

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_circuit_breaker_options, Enum.sort_by(unknown, &inspect/1)}}
  end

  defp valid_threshold?(value),
    do: is_integer(value) and value > 0 and value <= @max_failure_threshold

  defp valid_probe_after?(value),
    do: is_integer(value) and value > 0 and value <= @max_probe_after_ms

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
