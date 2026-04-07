defmodule Favn.Window.Anchor do
  @moduledoc """
  Run-level anchor window request.

  An anchor window represents execution intent from scheduler/operator/pipeline.
  """

  alias Favn.Window.Key
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month

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

  @spec validate(t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = anchor) do
    with :ok <- Validate.kind(anchor.kind),
         :ok <- validate_order(anchor.start_at, anchor.end_at),
         :ok <- Validate.timezone(anchor.timezone),
         :ok <- validate_key(anchor) do
      :ok
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
         :ok <- validate_order(start_at, end_at) do
      current = floor_to_kind(start_at, kind, timezone)
      boundary = floor_to_kind(end_at, kind, timezone)

      anchors =
        Stream.unfold(current, fn cursor ->
          if DateTime.compare(cursor, boundary) == :lt do
            next = shift_kind(cursor, kind, 1)
            {__MODULE__.new!(kind, cursor, next, timezone: timezone), next}
          else
            nil
          end
        end)
        |> Enum.to_list()

      {:ok, anchors}
    end
  end

  defp floor_to_kind(datetime, :hour, timezone),
    do:
      datetime
      |> DateTime.shift_zone!(timezone)
      |> Map.merge(%{minute: 0, second: 0, microsecond: {0, 0}})

  defp floor_to_kind(datetime, :day, timezone),
    do:
      datetime
      |> DateTime.shift_zone!(timezone)
      |> Map.merge(%{hour: 0, minute: 0, second: 0, microsecond: {0, 0}})

  defp floor_to_kind(datetime, :month, timezone),
    do:
      datetime
      |> DateTime.shift_zone!(timezone)
      |> Map.merge(%{day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 0}})

  defp shift_kind(datetime, :hour, count), do: DateTime.add(datetime, count * 3600, :second)
  defp shift_kind(datetime, :day, count), do: DateTime.add(datetime, count, :day)

  defp shift_kind(%DateTime{} = datetime, :month, count) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    year = div(total, 12)
    month = rem(total, 12) + 1
    {:ok, new_date} = Date.new(year, month, 1)
    {:ok, naive} = NaiveDateTime.new(new_date, ~T[00:00:00.000000])
    DateTime.from_naive!(naive, datetime.time_zone)
  end
end
