defmodule Favn.ResourceRecovery.Policy do
  @moduledoc """
  Opt-in pipeline policy for recovery after a shared resource becomes healthy.

  Recovery never mutates a terminal source run. `:retry_remaining` creates a
  linked rerun for explicitly safe failed nodes and nodes that never started
  because the resource circuit was open.
  """

  @max_age_ms 30 * 24 * 60 * 60 * 1_000

  @enforce_keys [:mode, :max_age_ms]
  defstruct [:mode, :max_age_ms]

  @type mode :: :retry_remaining
  @type t :: %__MODULE__{mode: mode(), max_age_ms: pos_integer()}

  @doc "Builds and validates a resource recovery policy."
  @spec new(atom() | String.t(), keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(mode, opts \\ [])

  def new(mode, opts) when is_list(opts) do
    if Keyword.keyword?(opts),
      do: new(mode, Map.new(opts)),
      else: {:error, {:invalid_resource_recovery_options, opts}}
  end

  def new(mode, opts) when is_map(opts) do
    with {:ok, mode} <- normalize_mode(mode),
         :ok <- reject_unknown_keys(opts),
         max_age_ms <- field(opts, :max_age_ms, 6 * 60 * 60 * 1_000),
         :ok <- validate_max_age(max_age_ms) do
      {:ok, %__MODULE__{mode: mode, max_age_ms: max_age_ms}}
    end
  end

  def new(_mode, opts), do: {:error, {:invalid_resource_recovery_options, opts}}

  @doc "Builds a resource recovery policy or raises `ArgumentError`."
  @spec new!(atom() | String.t(), keyword() | map()) :: t()
  def new!(mode, opts \\ []) do
    case new(mode, opts) do
      {:ok, policy} ->
        policy

      {:error, reason} ->
        raise ArgumentError, "invalid resource recovery policy: #{inspect(reason)}"
    end
  end

  @doc "Normalizes a persisted policy value."
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = policy), do: new(policy.mode, %{max_age_ms: policy.max_age_ms})

  def from_value(value) when is_map(value) do
    new(field(value, :mode), %{max_age_ms: field(value, :max_age_ms)})
  end

  def from_value(value), do: {:error, {:invalid_resource_recovery_policy, value}}

  @doc "Normalizes a persisted policy value or raises `ArgumentError`."
  @spec from_value!(term()) :: t() | nil
  def from_value!(value) do
    case from_value(value) do
      {:ok, policy} ->
        policy

      {:error, reason} ->
        raise ArgumentError, "invalid resource recovery policy: #{inspect(reason)}"
    end
  end

  defp normalize_mode(:retry_remaining), do: {:ok, :retry_remaining}
  defp normalize_mode("retry_remaining"), do: {:ok, :retry_remaining}
  defp normalize_mode(mode), do: {:error, {:invalid_resource_recovery_mode, mode}}

  defp validate_max_age(value) when is_integer(value) and value > 0 and value <= @max_age_ms,
    do: :ok

  defp validate_max_age(value), do: {:error, {:invalid_resource_recovery_max_age_ms, value}}

  defp reject_unknown_keys(value) do
    allowed = MapSet.new([:max_age_ms, "max_age_ms"])
    unknown = value |> Map.keys() |> Enum.reject(&MapSet.member?(allowed, &1))

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_resource_recovery_options, Enum.sort_by(unknown, &inspect/1)}}
  end

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
