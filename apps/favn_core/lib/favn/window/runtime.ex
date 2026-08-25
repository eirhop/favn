defmodule Favn.Window.Runtime do
  @moduledoc """
  Concrete runtime window used by one asset execution node.
  """

  alias Favn.Window.Key
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month | :year

  @type t :: %__MODULE__{
          kind: kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t(),
          key: Key.t(),
          anchor_key: Key.t(),
          logical_window_count: pos_integer()
        }

  defstruct [
    :kind,
    :start_at,
    :end_at,
    :key,
    :anchor_key,
    timezone: "Etc/UTC",
    logical_window_count: 1
  ]

  @spec new(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def new(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ [])
      when is_map(anchor_key) and is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         :ok <- Validate.kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- validate_order(start_at, end_at),
         :ok <- Validate.timezone(timezone),
         :ok <- validate_key(anchor_key),
         {:ok, key} <- Key.new(kind, start_at, timezone) do
      {:ok,
       %__MODULE__{
         kind: kind,
         start_at: start_at,
         end_at: end_at,
         timezone: timezone,
         key: key,
         anchor_key: anchor_key,
         logical_window_count: 1
       }}
    end
  end

  @spec new!(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) :: t()
  def new!(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    case new(kind, start_at, end_at, anchor_key, opts) do
      {:ok, runtime} -> runtime
      {:error, reason} -> raise ArgumentError, "invalid runtime window: #{inspect(reason)}"
    end
  end

  @doc "Builds one runtime window whose identity includes both physical range bounds."
  @spec new_range(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def new_range(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ [])
      when is_map(anchor_key) and is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone, :logical_window_count]),
         :ok <- Validate.kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         logical_window_count <- Keyword.get(opts, :logical_window_count),
         :ok <- validate_logical_window_count(logical_window_count, :range),
         :ok <- validate_order(start_at, end_at),
         :ok <- Validate.timezone(timezone),
         :ok <- validate_key(anchor_key),
         {:ok, key} <- Key.new_range(kind, start_at, end_at, timezone) do
      {:ok,
       %__MODULE__{
         kind: kind,
         start_at: start_at,
         end_at: end_at,
         timezone: timezone,
         key: key,
         anchor_key: anchor_key,
         logical_window_count: logical_window_count
       }}
    end
  end

  @doc "Builds a range runtime window, raising when invalid."
  @spec new_range!(kind(), DateTime.t(), DateTime.t(), Key.t(), keyword()) :: t()
  def new_range!(kind, %DateTime{} = start_at, %DateTime{} = end_at, anchor_key, opts \\ []) do
    case new_range(kind, start_at, end_at, anchor_key, opts) do
      {:ok, runtime} -> runtime
      {:error, reason} -> raise ArgumentError, "invalid runtime window range: #{inspect(reason)}"
    end
  end

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = runtime) do
    with :ok <- Validate.kind(runtime.kind),
         :ok <- validate_order(runtime.start_at, runtime.end_at),
         :ok <- Validate.timezone(runtime.timezone),
         :ok <- validate_key(runtime.key),
         :ok <- validate_key(runtime.anchor_key),
         :ok <-
           validate_logical_window_count(
             runtime.logical_window_count,
             if(Key.range?(runtime.key), do: :range, else: :exact)
           ) do
      expected =
        if Key.range?(runtime.key) do
          Key.new_range!(runtime.kind, runtime.start_at, runtime.end_at, runtime.timezone)
        else
          Key.new!(runtime.kind, runtime.start_at, runtime.timezone)
        end

      if runtime.key == expected, do: :ok, else: {:error, :invalid_key}
    end
  end

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _ -> {:error, :invalid_window_bounds}
    end
  end

  defp validate_key(%{kind: kind, start_at_us: start_at_us, timezone: timezone})
       when kind in [:hour, :day, :month, :year] and is_integer(start_at_us) and
              is_binary(timezone) do
    Key.validate(%{kind: kind, start_at_us: start_at_us, timezone: timezone})
  end

  defp validate_key(value), do: Key.validate(value)

  defp validate_logical_window_count(value, :range) when is_integer(value) and value > 1,
    do: :ok

  defp validate_logical_window_count(1, :exact), do: :ok
  defp validate_logical_window_count(_value, _mode), do: {:error, :invalid_logical_window_count}
end
