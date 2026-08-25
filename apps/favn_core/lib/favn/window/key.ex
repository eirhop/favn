defmodule Favn.Window.Key do
  @moduledoc """
  Canonical runtime window identity.

  Keys are structured for internal logic and can also be encoded into stable
  strings for storage and indexing.
  """

  alias Favn.Window.{Anchor, Runtime}
  alias Favn.Window.Validate

  @type kind :: :hour | :day | :month | :year

  @type t :: %{
          optional(:end_at_us) => integer(),
          kind: kind(),
          start_at_us: integer(),
          timezone: String.t()
        }

  @spec new(kind(), DateTime.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def new(kind, %DateTime{} = start_at, timezone) do
    with :ok <- Validate.kind(kind),
         :ok <- Validate.timezone(timezone) do
      {:ok,
       %{
         kind: kind,
         start_at_us: DateTime.to_unix(start_at, :microsecond),
         timezone: timezone
       }}
    end
  end

  @spec new!(kind(), DateTime.t(), String.t()) :: t()
  def new!(kind, %DateTime{} = start_at, timezone) do
    case new(kind, start_at, timezone) do
      {:ok, key} -> key
      {:error, reason} -> raise ArgumentError, "invalid window key: #{inspect(reason)}"
    end
  end

  @doc "Builds a canonical identity for one physical range spanning adjacent logical windows."
  @spec new_range(kind(), DateTime.t(), DateTime.t(), String.t()) ::
          {:ok, t()} | {:error, term()}
  def new_range(kind, %DateTime{} = start_at, %DateTime{} = end_at, timezone) do
    with {:ok, key} <- new(kind, start_at, timezone),
         :lt <- DateTime.compare(start_at, end_at) do
      {:ok, Map.put(key, :end_at_us, DateTime.to_unix(end_at, :microsecond))}
    else
      comparison when comparison in [:eq, :gt] -> {:error, :invalid_window_bounds}
      {:error, _reason} = error -> error
    end
  end

  @doc "Builds a range identity, raising when invalid."
  @spec new_range!(kind(), DateTime.t(), DateTime.t(), String.t()) :: t()
  def new_range!(kind, %DateTime{} = start_at, %DateTime{} = end_at, timezone) do
    case new_range(kind, start_at, end_at, timezone) do
      {:ok, key} -> key
      {:error, reason} -> raise ArgumentError, "invalid window range key: #{inspect(reason)}"
    end
  end

  @doc "Returns whether the identity names a multi-window physical range."
  @spec range?(term()) :: boolean()
  def range?(%{end_at_us: end_at_us}) when is_integer(end_at_us), do: true
  def range?(_key), do: false

  @doc """
  Build a key from an anchor or runtime window struct.
  """
  @spec from_window(Anchor.t() | Runtime.t()) :: t()
  def from_window(%Anchor{kind: kind, start_at: start_at, timezone: timezone}) do
    new!(kind, start_at, timezone)
  end

  def from_window(%Runtime{kind: kind, start_at: start_at, timezone: timezone}) do
    new!(kind, start_at, timezone)
  end

  @spec encode(t()) :: String.t()
  def encode(%{kind: kind, start_at_us: start_at_us, end_at_us: end_at_us, timezone: timezone})
      when kind in [:hour, :day, :month, :year] and is_integer(start_at_us) and
             is_integer(end_at_us) and is_binary(timezone) do
    start_at = DateTime.from_unix!(start_at_us, :microsecond)
    end_at = DateTime.from_unix!(end_at_us, :microsecond)
    "range:#{kind}:#{timezone}:#{DateTime.to_iso8601(start_at)}/#{DateTime.to_iso8601(end_at)}"
  end

  def encode(%{kind: kind, start_at_us: start_at_us, timezone: timezone})
      when kind in [:hour, :day, :month, :year] and is_integer(start_at_us) and
             is_binary(timezone) do
    dt = DateTime.from_unix!(start_at_us, :microsecond)
    "#{kind}:#{timezone}:#{DateTime.to_iso8601(dt)}"
  end

  @spec decode(String.t()) :: {:ok, t()} | {:error, term()}
  def decode("range:" <> value) do
    case String.split(value, ":", parts: 3) do
      [kind_raw, timezone, bounds] ->
        with [start_raw, end_raw] <- String.split(bounds, "/", parts: 2),
             {:ok, kind} <- decode_kind(kind_raw),
             {:ok, start_at, _offset} <- DateTime.from_iso8601(start_raw),
             {:ok, end_at, _offset} <- DateTime.from_iso8601(end_raw),
             :ok <- Validate.timezone(timezone) do
          new_range(kind, start_at, end_at, timezone)
        else
          [_single] -> {:error, {:invalid_encoded_key, "range:" <> value}}
          {:error, reason} -> {:error, reason}
        end

      _other ->
        {:error, {:invalid_encoded_key, "range:" <> value}}
    end
  end

  def decode(value) when is_binary(value) do
    case String.split(value, ":", parts: 3) do
      [kind_raw, timezone, datetime_raw] ->
        with {:ok, kind} <- decode_kind(kind_raw),
             {:ok, dt, _offset} <- DateTime.from_iso8601(datetime_raw),
             :ok <- Validate.timezone(timezone) do
          new(kind, dt, timezone)
        else
          {:error, reason} -> {:error, reason}
        end

      _other ->
        {:error, {:invalid_encoded_key, value}}
    end
  end

  def decode(value), do: {:error, {:invalid_encoded_key, value}}

  @spec validate(term()) :: :ok | {:error, term()}
  def validate(%{
        kind: kind,
        start_at_us: start_at_us,
        end_at_us: end_at_us,
        timezone: timezone
      }) do
    with :ok <- Validate.kind(kind),
         true <- is_integer(start_at_us) and is_integer(end_at_us) and start_at_us < end_at_us,
         :ok <- Validate.timezone(timezone) do
      :ok
    else
      false -> {:error, :invalid_key}
      {:error, reason} -> {:error, reason}
    end
  end

  def validate(%{kind: kind, start_at_us: start_at_us, timezone: timezone}) do
    with :ok <- Validate.kind(kind),
         true <- is_integer(start_at_us),
         :ok <- Validate.timezone(timezone) do
      :ok
    else
      false -> {:error, :invalid_key}
      {:error, reason} -> {:error, reason}
    end
  end

  def validate(_value), do: {:error, :invalid_key}

  defp decode_kind("hour"), do: {:ok, :hour}
  defp decode_kind("day"), do: {:ok, :day}
  defp decode_kind("month"), do: {:ok, :month}
  defp decode_kind("year"), do: {:ok, :year}
  defp decode_kind(other), do: {:error, {:invalid_kind, other}}
end
