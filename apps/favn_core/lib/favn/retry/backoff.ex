defmodule Favn.Retry.Backoff do
  @moduledoc """
  Serializable timing policy for automatic node-attempt retries.

  Backoff controls only the delay between attempts of one asset node. It does
  not configure SQL safety retries, persistence retries, schedule overlap, or
  reruns.
  """

  @max_delay_ms 86_400_000

  @type strategy :: :fixed | :exponential
  @type t :: %__MODULE__{
          strategy: strategy(),
          initial_ms: non_neg_integer(),
          max_ms: non_neg_integer(),
          jitter: float()
        }

  @enforce_keys [:strategy, :initial_ms, :max_ms, :jitter]
  defstruct strategy: :fixed, initial_ms: 0, max_ms: 0, jitter: 0.0

  @doc "Returns the largest supported delay, including a retry-after hint."
  @spec max_delay_ms() :: pos_integer()
  def max_delay_ms, do: @max_delay_ms

  @doc "Builds and validates a backoff policy."
  @spec new(term()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = backoff), do: validate(backoff)

  def new(delay_ms) when is_integer(delay_ms),
    do: new(%{strategy: :fixed, initial_ms: delay_ms, max_ms: delay_ms, jitter: 0.0})

  def new({:fixed, opts}) when is_list(opts) do
    delay = Keyword.get(opts, :delay, Keyword.get(opts, :initial, 0))

    new(%{
      strategy: :fixed,
      initial_ms: delay,
      max_ms: delay,
      jitter: Keyword.get(opts, :jitter, 0.0)
    })
  end

  def new({:exponential, opts}) when is_list(opts) do
    new(%{
      strategy: :exponential,
      initial_ms: Keyword.get(opts, :initial),
      max_ms: Keyword.get(opts, :max),
      jitter: Keyword.get(opts, :jitter, 0.0)
    })
  end

  def new(value) when is_list(value) do
    if Keyword.keyword?(value), do: new(Map.new(value)), else: {:error, {:invalid_backoff, value}}
  end

  def new(value) when is_map(value) do
    strategy = field(value, :strategy, :fixed)
    initial_ms = field(value, :initial_ms, field(value, :delay_ms, 0))
    max_ms = field(value, :max_ms, initial_ms)
    jitter = field(value, :jitter, 0.0)

    validate(%__MODULE__{
      strategy: decode_strategy(strategy),
      initial_ms: initial_ms,
      max_ms: max_ms,
      jitter: normalize_jitter(jitter)
    })
  end

  def new(value), do: {:error, {:invalid_backoff, value}}

  @doc "Validates an already-normalized policy."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = backoff) do
    cond do
      backoff.strategy not in [:fixed, :exponential] ->
        {:error, {:invalid_backoff_strategy, backoff.strategy}}

      not valid_delay?(backoff.initial_ms) ->
        {:error, {:invalid_backoff_initial_ms, backoff.initial_ms}}

      not valid_delay?(backoff.max_ms) ->
        {:error, {:invalid_backoff_max_ms, backoff.max_ms}}

      backoff.max_ms < backoff.initial_ms ->
        {:error, {:invalid_backoff_bounds, backoff.initial_ms, backoff.max_ms}}

      backoff.strategy == :fixed and backoff.max_ms != backoff.initial_ms ->
        {:error, {:invalid_fixed_backoff_bounds, backoff.initial_ms, backoff.max_ms}}

      not (is_float(backoff.jitter) and backoff.jitter >= 0.0 and backoff.jitter <= 1.0) ->
        {:error, {:invalid_backoff_jitter, backoff.jitter}}

      true ->
        {:ok, backoff}
    end
  end

  @doc """
  Calculates a bounded delay after `failed_attempt`.

  `sample` is in `0.0..1.0` and exists so callers can test jitter without
  depending on random process state. A retry-after hint raises the delay when
  it is larger than policy backoff, but never above the global bound.
  """
  @spec delay_ms(t(), pos_integer(), non_neg_integer() | nil, float()) :: non_neg_integer()
  def delay_ms(%__MODULE__{} = backoff, failed_attempt, retry_after_ms \\ nil, sample \\ 0.5)
      when is_integer(failed_attempt) and failed_attempt > 0 do
    base = base_delay(backoff, failed_attempt)
    jittered = jitter(base, backoff.jitter, sample)
    retry_after_ms = bounded_retry_after(retry_after_ms)
    min(max(jittered, retry_after_ms), @max_delay_ms)
  end

  defp base_delay(%__MODULE__{strategy: :fixed, initial_ms: delay}, _attempt), do: delay

  defp base_delay(%__MODULE__{strategy: :exponential} = backoff, failed_attempt) do
    multiplier = Integer.pow(2, min(failed_attempt - 1, 30))
    min(backoff.initial_ms * multiplier, backoff.max_ms)
  end

  defp jitter(delay, ratio, _sample) when ratio == 0.0, do: delay

  defp jitter(delay, ratio, sample) do
    sample = if is_number(sample), do: min(max(sample * 1.0, 0.0), 1.0), else: 0.5
    factor = 1.0 + ratio * (2.0 * sample - 1.0)
    delay |> Kernel.*(factor) |> round() |> max(0)
  end

  defp bounded_retry_after(value) when is_integer(value) and value >= 0,
    do: min(value, @max_delay_ms)

  defp bounded_retry_after(_value), do: 0

  defp valid_delay?(value),
    do: is_integer(value) and value >= 0 and value <= @max_delay_ms

  defp field(map, key, default) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp decode_strategy("fixed"), do: :fixed
  defp decode_strategy("exponential"), do: :exponential
  defp decode_strategy(value), do: value

  defp normalize_jitter(value) when is_integer(value), do: value * 1.0
  defp normalize_jitter(value), do: value
end
