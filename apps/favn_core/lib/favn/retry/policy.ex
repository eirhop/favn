defmodule Favn.Retry.Policy do
  @moduledoc """
  Canonical node-attempt retry policy shared by authoring and runtime.

  `max_attempts` includes the initial attempt. The default is one, which means
  no automatic node retry. A policy decides how often and when a node may be
  attempted; the normalized runner failure independently decides whether a
  repeat is safe.
  """

  alias Favn.Retry.Backoff

  @max_attempts 100

  @type t :: %__MODULE__{max_attempts: pos_integer(), backoff: Backoff.t()}

  @enforce_keys [:max_attempts, :backoff]
  defstruct max_attempts: 1,
            backoff: %Backoff{strategy: :fixed, initial_ms: 0, max_ms: 0, jitter: 0.0}

  @doc "Returns the default one-attempt policy."
  @spec default() :: t()
  def default, do: %__MODULE__{max_attempts: 1, backoff: default_backoff()}

  @doc "Returns the maximum supported attempt count."
  @spec max_attempts_limit() :: pos_integer()
  def max_attempts_limit, do: @max_attempts

  @doc "Builds and validates a policy from a map or keyword list."
  @spec new(term()) :: {:ok, t()} | {:error, term()}
  def new(nil), do: {:ok, default()}
  def new(%__MODULE__{} = policy), do: validate(policy)

  def new(value) when is_list(value) do
    if Keyword.keyword?(value),
      do: new(Map.new(value)),
      else: {:error, {:invalid_retry_policy, value}}
  end

  def new(value) when is_map(value) do
    max_attempts = field(value, :max_attempts, 1)
    backoff_value = field(value, :backoff, 0)

    with {:ok, backoff} <- Backoff.new(backoff_value) do
      validate(%__MODULE__{max_attempts: max_attempts, backoff: backoff})
    end
  end

  def new(value), do: {:error, {:invalid_retry_policy, value}}

  @doc "Builds a policy and raises `ArgumentError` with a stable reason."
  @spec new!(term()) :: t()
  def new!(value) do
    case new(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid retry policy: #{inspect(reason)}"
    end
  end

  @doc "Validates an already-normalized policy."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = policy) do
    cond do
      not (is_integer(policy.max_attempts) and policy.max_attempts > 0 and
               policy.max_attempts <= @max_attempts) ->
        {:error, {:invalid_retry_max_attempts, policy.max_attempts}}

      true ->
        with {:ok, backoff} <- Backoff.validate(policy.backoff) do
          {:ok, %{policy | backoff: backoff}}
        end
    end
  end

  @doc "Calculates the delay after a failed attempt."
  @spec delay_ms(t(), pos_integer(), non_neg_integer() | nil, float()) :: non_neg_integer()
  def delay_ms(
        %__MODULE__{backoff: backoff},
        failed_attempt,
        retry_after_ms \\ nil,
        sample \\ 0.5
      ),
      do: Backoff.delay_ms(backoff, failed_attempt, retry_after_ms, sample)

  defp default_backoff,
    do: %Backoff{strategy: :fixed, initial_ms: 0, max_ms: 0, jitter: 0.0}

  defp field(map, key, default),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
