defmodule Favn.SQL.Retry.Policy do
  @moduledoc """
  Bounded retry policy for SQL runtime operations.

  The defaults favor short transient retries and slower capacity retries. Callers
  may inject sleep and randomness through `Favn.SQL.Retry.run/2` for deterministic
  tests.
  """

  alias Favn.SQL.Retry.Classification

  defstruct max_attempts: 3,
            base_delay_ms: 50,
            max_delay_ms: 1_000,
            capacity_base_delay_ms: 250,
            capacity_max_delay_ms: 5_000,
            jitter: 0.2

  @type t :: %__MODULE__{
          max_attempts: pos_integer(),
          base_delay_ms: non_neg_integer(),
          max_delay_ms: pos_integer(),
          capacity_base_delay_ms: non_neg_integer(),
          capacity_max_delay_ms: pos_integer(),
          jitter: float()
        }

  @doc """
  Builds a policy from a struct or keyword overrides.
  """
  @spec new(t() | keyword() | nil) :: t()
  def new(%__MODULE__{} = policy), do: policy
  def new(nil), do: %__MODULE__{}

  def new(opts) when is_list(opts) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Returns the delay before retry attempt `attempt`.
  """
  @spec delay_ms(t(), Classification.t(), pos_integer(), (-> float())) :: non_neg_integer()
  def delay_ms(%__MODULE__{} = policy, %Classification{} = classification, attempt, random_fun)
      when is_integer(attempt) and attempt >= 1 and is_function(random_fun, 0) do
    {base, max_delay} = delay_bounds(policy, classification.class)

    base
    |> exponential_delay(attempt)
    |> min(max_delay)
    |> apply_jitter(policy.jitter, random_fun.())
    |> min(max_delay)
    |> max(0)
  end

  defp delay_bounds(%__MODULE__{} = policy, :capacity),
    do: {policy.capacity_base_delay_ms, policy.capacity_max_delay_ms}

  defp delay_bounds(%__MODULE__{} = policy, _class),
    do: {policy.base_delay_ms, policy.max_delay_ms}

  defp exponential_delay(base, attempt), do: round(base * :math.pow(2, attempt - 1))

  defp apply_jitter(delay, jitter, random_value) when jitter > 0 do
    multiplier = 1 + (clamp(random_value, 0.0, 1.0) - 0.5) * 2 * jitter
    round(delay * multiplier)
  end

  defp apply_jitter(delay, _jitter, _random_value), do: delay

  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value
end
