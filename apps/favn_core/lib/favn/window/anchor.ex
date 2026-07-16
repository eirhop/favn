defmodule Favn.Window.Anchor do
  @moduledoc """
  Run-level anchor window request.

  An anchor window represents execution intent from scheduler/operator/pipeline.
  """

  alias Favn.TimePeriod
  alias Favn.Window.Key
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month | :year

  @type t :: %__MODULE__{
          kind: kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t(),
          key: Key.t()
        }

  defstruct [:kind, :start_at, :end_at, :key, timezone: "Etc/UTC"]

  @spec new(kind(), DateTime.t(), DateTime.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) when is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         :ok <- Validate.kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- validate_order(start_at, end_at),
         :ok <- Validate.timezone(timezone),
         {:ok, key} <- Key.new(kind, start_at, timezone) do
      {:ok,
       %__MODULE__{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone, key: key}}
    end
  end

  @spec new!(kind(), DateTime.t(), DateTime.t(), keyword()) :: t()
  def new!(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ []) do
    case new(kind, start_at, end_at, opts) do
      {:ok, anchor} -> anchor
      {:error, reason} -> raise ArgumentError, "invalid anchor window: #{inspect(reason)}"
    end
  end

  @doc """
  Rehydrates and validates an anchor from a struct or JSON-shaped map.
  """
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}

  def from_value(%__MODULE__{} = anchor) do
    case validate(anchor) do
      :ok -> {:ok, anchor}
      {:error, _reason} = error -> error
    end
  end

  def from_value(value) when is_map(value) do
    timezone = field(value, :timezone, "Etc/UTC")

    with {:ok, kind} <- decode_kind(field(value, :kind)),
         {:ok, start_at} <- decode_datetime(field(value, :start_at), timezone),
         {:ok, end_at} <- decode_datetime(field(value, :end_at), timezone) do
      new(kind, start_at, end_at, timezone: timezone)
    end
  end

  def from_value(value), do: {:error, {:invalid_anchor_window, value}}

  @doc """
  Rehydrates an anchor, raising `ArgumentError` when it is invalid.
  """
  @spec from_value!(term()) :: t() | nil
  def from_value!(value) do
    case from_value(value) do
      {:ok, anchor} -> anchor
      {:error, reason} -> raise ArgumentError, "invalid anchor window: #{inspect(reason)}"
    end
  end

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = anchor) do
    with :ok <- Validate.kind(anchor.kind),
         :ok <- validate_order(anchor.start_at, anchor.end_at),
         :ok <- Validate.timezone(anchor.timezone) do
      validate_key(anchor)
    end
  end

  defp validate_key(%__MODULE__{} = anchor) do
    expected = Key.new!(anchor.kind, anchor.start_at, anchor.timezone)

    with :ok <- Key.validate(anchor.key) do
      if anchor.key == expected, do: :ok, else: {:error, :invalid_key}
    end
  end

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _ -> {:error, :invalid_window_bounds}
    end
  end

  defp decode_kind(kind) when kind in [:hour, "hour"], do: {:ok, :hour}
  defp decode_kind(kind) when kind in [:day, "day"], do: {:ok, :day}
  defp decode_kind(kind) when kind in [:month, "month"], do: {:ok, :month}
  defp decode_kind(kind) when kind in [:year, "year"], do: {:ok, :year}
  defp decode_kind(kind), do: {:error, {:invalid_window_kind, kind}}

  defp decode_datetime(%DateTime{} = datetime, timezone),
    do: DateTime.shift_zone(datetime, timezone, Favn.Timezone.database!())

  defp decode_datetime(value, timezone) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        DateTime.shift_zone(datetime, timezone, Favn.Timezone.database!())

      {:error, reason} ->
        {:error, {:invalid_window_datetime, value, reason}}
    end
  end

  defp decode_datetime(value, _timezone), do: {:error, {:invalid_window_datetime, value}}

  defp field(value, key, default \\ nil),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  @doc """
  Expand a time range into contiguous anchor windows of the given kind.

  `end_at` is treated as exclusive.
  """
  @spec expand_range(kind(), DateTime.t(), DateTime.t(), keyword()) ::
          {:ok, [t()]} | {:error, term()}
  def expand_range(kind, %DateTime{} = start_at, %DateTime{} = end_at, opts \\ [])
      when is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         :ok <- Validate.kind(kind),
         timezone <- Keyword.get(opts, :timezone, "Etc/UTC"),
         :ok <- Validate.timezone(timezone),
         :ok <- validate_order(start_at, end_at),
         {:ok, periods} <- TimePeriod.expand_range(kind, start_at, end_at, timezone) do
      periods
      |> Enum.map(fn period ->
        new!(period.kind, period.start_at, period.end_at, timezone: period.timezone)
      end)
      |> then(&{:ok, &1})
    end
  end
end
