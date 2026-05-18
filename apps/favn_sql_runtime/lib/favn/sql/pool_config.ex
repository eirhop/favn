defmodule Favn.SQL.PoolConfig do
  @moduledoc """
  Normalized runtime configuration for local SQL session pooling.

  Pooling is local to the current BEAM. It does not coordinate checked-out or
  idle sessions across nodes.
  """

  alias Favn.SQL.Error

  @default_enabled true
  @default_max_idle_per_key 1
  @default_idle_timeout_ms 300_000
  @allowed_keys [:enabled, :max_idle_per_key, :idle_timeout_ms]

  defstruct enabled: @default_enabled,
            max_idle_per_key: @default_max_idle_per_key,
            idle_timeout_ms: @default_idle_timeout_ms

  @type t :: %__MODULE__{
          enabled: boolean(),
          max_idle_per_key: non_neg_integer(),
          idle_timeout_ms: pos_integer()
        }

  @doc """
  Parses public SQL pool runtime configuration.

  Missing or `nil` configuration resolves to the default enabled pool:
  `enabled: true`, `max_idle_per_key: 1`, and `idle_timeout_ms: 300_000`.
  """
  @spec parse(nil | keyword() | map()) :: {:ok, t()} | {:error, Error.t()}
  def parse(nil), do: {:ok, defaults()}

  def parse(%__MODULE__{} = config), do: {:ok, config}

  def parse(config) when is_list(config) do
    if Keyword.keyword?(config) do
      config |> Map.new() |> parse_map()
    else
      {:error,
       invalid_error("pool config must be a keyword list or map", %{reason: :expected_keyword})}
    end
  end

  def parse(config) when is_map(config), do: parse_map(config)

  def parse(_config) do
    {:error, invalid_error("pool config must be a keyword list or map", %{reason: :expected_map})}
  end

  defp parse_map(config) do
    keys = Map.keys(config)
    unknown = keys -- @allowed_keys

    if unknown == [] do
      normalize(config)
    else
      {:error, invalid_error("pool config contains unknown keys", %{keys: Enum.sort(unknown)})}
    end
  end

  defp normalize(config) do
    enabled = Map.get(config, :enabled, @default_enabled)
    max_idle_per_key = Map.get(config, :max_idle_per_key, @default_max_idle_per_key)
    idle_timeout_ms = Map.get(config, :idle_timeout_ms, @default_idle_timeout_ms)

    cond do
      not is_boolean(enabled) ->
        {:error, invalid_error("pool enabled must be a boolean", %{key: :enabled})}

      not (is_integer(max_idle_per_key) and max_idle_per_key >= 0) ->
        {:error,
         invalid_error("pool max_idle_per_key must be a non-negative integer", %{
           key: :max_idle_per_key
         })}

      not (is_integer(idle_timeout_ms) and idle_timeout_ms > 0) ->
        {:error,
         invalid_error("pool idle_timeout_ms must be a positive integer", %{
           key: :idle_timeout_ms
         })}

      true ->
        {:ok,
         %__MODULE__{
           enabled: enabled,
           max_idle_per_key: max_idle_per_key,
           idle_timeout_ms: idle_timeout_ms
         }}
    end
  end

  defp defaults do
    %__MODULE__{
      enabled: @default_enabled,
      max_idle_per_key: @default_max_idle_per_key,
      idle_timeout_ms: @default_idle_timeout_ms
    }
  end

  defp invalid_error(message, details) do
    %Error{type: :invalid_config, message: message, operation: :pool_config, details: details}
  end
end
