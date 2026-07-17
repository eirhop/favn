defmodule Favn.Freshness.Key do
  @moduledoc """
  Stable string keys for freshness state.

  Freshness keys identify the grain of freshness information that later planner,
  executor, and storage code can share without duplicating key formatting rules.

  The encoded forms are intentionally explicit and human-readable:

    * `"latest"`
    * `"window:<encoded-window-key>"`
    * `"window:<encoded-window-key>|calendar:<kind>:<timezone>:<period-start>"`
    * `"calendar:<kind>:<timezone>:<period-start>"`

  Window keys delegate the nested key format to `Favn.Window.Key.encode/1`.
  Window-refresh keys combine that exact data window with a calendar refresh
  period so, for example, June and July can each be refreshed once on the same
  local day. Calendar keys use local period starts for the supplied timezone:
  hourly values built from absolute datetimes are `YYYY-MM-DDTHH±HH:MM`, so
  repeated daylight-saving hours remain distinct. Legacy unqualified
  `YYYY-MM-DDTHH` values remain valid. Daily values are `YYYY-MM-DD`, monthly
  values are `YYYY-MM`, and yearly values are `YYYY`.

  Authors usually do not build these keys directly. They are useful when reading
  internal orchestrator freshness state through `FavnOrchestrator` or when tests
  need exact keys for `FavnOrchestrator.AssetFreshnessState`.

  ## Examples

      iex> Favn.Freshness.Key.latest()
      "latest"

      iex> Favn.Freshness.Key.calendar!(:day, "Etc/UTC", ~D[2026-05-09])
      "calendar:day:Etc/UTC:2026-05-09"
  """

  alias Favn.Window.{Key, Policy, Validate}

  @typedoc "Supported calendar freshness period kinds."
  @type kind :: :hour | :day | :month | :year

  @typedoc "Canonical encoded freshness key."
  @type t :: String.t()

  @doc """
  Returns the key for latest/full-load freshness state.
  """
  @spec latest() :: t()
  def latest, do: "latest"

  @doc """
  Wraps a canonical window key as a freshness key.
  """
  @spec window(Key.t()) :: {:ok, t()} | {:error, term()}
  def window(window_key) do
    with :ok <- Key.validate(window_key) do
      {:ok, "window:" <> Key.encode(window_key)}
    end
  end

  @doc """
  Wraps a canonical window key as a freshness key, raising on invalid input.
  """
  @spec window!(Key.t()) :: t()
  def window!(window_key) do
    case window(window_key) do
      {:ok, key} -> key
      {:error, reason} -> raise ArgumentError, "invalid freshness window key: #{inspect(reason)}"
    end
  end

  @doc """
  Combines an exact window key with a calendar refresh period.

  This is the persisted identity used by windowed assets whose
  `%Favn.Window.Spec{}` declares `refresh_from`. The exact window remains part of
  the key, so a success for one lookback window never satisfies another.
  """
  @spec window_refresh(
          Key.t(),
          atom(),
          String.t(),
          Date.t() | NaiveDateTime.t() | DateTime.t() | String.t()
        ) :: {:ok, t()} | {:error, term()}
  def window_refresh(window_key, kind, timezone, period_start) do
    with :ok <- Key.validate(window_key),
         {:ok, calendar_key} <- calendar(kind, timezone, period_start) do
      {:ok, "window:#{Key.encode(window_key)}|#{calendar_key}"}
    end
  end

  @doc """
  Builds a window-refresh key, raising on invalid input.
  """
  @spec window_refresh!(
          Key.t(),
          atom(),
          String.t(),
          Date.t() | NaiveDateTime.t() | DateTime.t() | String.t()
        ) :: t()
  def window_refresh!(window_key, kind, timezone, period_start) do
    case window_refresh(window_key, kind, timezone, period_start) do
      {:ok, key} ->
        key

      {:error, reason} ->
        raise ArgumentError, "invalid freshness window refresh key: #{inspect(reason)}"
    end
  end

  @doc """
  Builds a calendar freshness key.

  `kind` accepts the same aliases as `Favn.Window.Policy.normalize_kind/1`.
  `timezone` is validated with the core window timezone validation. The period
  start may be a `Date`, `NaiveDateTime`, `DateTime`, or a preformatted period
  string matching the normalized kind.
  """
  @spec calendar(atom(), String.t(), Date.t() | NaiveDateTime.t() | DateTime.t() | String.t()) ::
          {:ok, t()} | {:error, term()}
  def calendar(kind, timezone, period_start) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- Validate.timezone(timezone),
         {:ok, value} <- format_period_start(kind, timezone, period_start) do
      {:ok, "calendar:#{kind}:#{timezone}:#{value}"}
    end
  end

  @doc """
  Builds a calendar freshness key, raising on invalid input.
  """
  @spec calendar!(atom(), String.t(), Date.t() | NaiveDateTime.t() | DateTime.t() | String.t()) ::
          t()
  def calendar!(kind, timezone, period_start) do
    case calendar(kind, timezone, period_start) do
      {:ok, key} ->
        key

      {:error, reason} ->
        raise ArgumentError, "invalid freshness calendar key: #{inspect(reason)}"
    end
  end

  @doc """
  Encodes a freshness key.

  Freshness keys are already represented as stable strings, so this function
  validates and returns the canonical encoded key.
  """
  @spec encode(t()) :: {:ok, t()} | {:error, term()}
  def encode(key), do: decode(key)

  @doc """
  Decodes and validates a freshness key string.

  Returns the canonical encoded key string on success.
  """
  @spec decode(term()) :: {:ok, t()} | {:error, term()}
  def decode("latest"), do: {:ok, latest()}

  def decode("window:" <> encoded_window_key) do
    case String.split(encoded_window_key, "|calendar:", parts: 2) do
      [encoded_window_key] ->
        with {:ok, window_key} <- Key.decode(encoded_window_key) do
          window(window_key)
        end

      [encoded_window_key, encoded_calendar_key] ->
        with {:ok, window_key} <- Key.decode(encoded_window_key),
             {:ok, calendar_key} <- decode("calendar:" <> encoded_calendar_key) do
          {:ok, "window:#{Key.encode(window_key)}|#{calendar_key}"}
        end
    end
  end

  def decode("calendar:" <> encoded_calendar_key) do
    case String.split(encoded_calendar_key, ":", parts: 3) do
      [kind_raw, timezone, value] -> decode_calendar(kind_raw, timezone, value)
      _other -> {:error, {:invalid_freshness_key, "calendar:" <> encoded_calendar_key}}
    end
  end

  def decode(value), do: {:error, {:invalid_freshness_key, value}}

  defp decode_calendar(kind_raw, timezone, value) do
    with {:ok, kind} <- normalize_kind(kind_raw),
         :ok <- Validate.timezone(timezone),
         {:ok, value} <- format_period_start(kind, timezone, value) do
      {:ok, "calendar:#{kind}:#{timezone}:#{value}"}
    end
  end

  defp normalize_kind("hour"), do: Policy.normalize_kind(:hour)
  defp normalize_kind("hourly"), do: Policy.normalize_kind(:hourly)
  defp normalize_kind("day"), do: Policy.normalize_kind(:day)
  defp normalize_kind("daily"), do: Policy.normalize_kind(:daily)
  defp normalize_kind("month"), do: Policy.normalize_kind(:month)
  defp normalize_kind("monthly"), do: Policy.normalize_kind(:monthly)
  defp normalize_kind("year"), do: Policy.normalize_kind(:year)
  defp normalize_kind("yearly"), do: Policy.normalize_kind(:yearly)

  defp normalize_kind(kind) when is_binary(kind),
    do: {:error, {:invalid_window_policy_kind, kind}}

  defp normalize_kind(kind), do: Policy.normalize_kind(kind)

  defp format_period_start(kind, _timezone, value) when is_binary(value) do
    if valid_period_string?(kind, value) do
      {:ok, value}
    else
      {:error, {:invalid_calendar_period_start, kind, value}}
    end
  end

  defp format_period_start(:hour, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(fn local ->
      offset = local.utc_offset + local.std_offset
      {:ok, format_hour(local.year, local.month, local.day, local.hour) <> format_offset(offset)}
    end)
  end

  defp format_period_start(:day, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> DateTime.to_date()
    |> Date.to_iso8601()
    |> then(&{:ok, &1})
  end

  defp format_period_start(:month, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(&{:ok, format_month(&1.year, &1.month)})
  end

  defp format_period_start(:year, timezone, %DateTime{} = datetime) do
    datetime
    |> DateTime.shift_zone!(timezone, Favn.Timezone.database!())
    |> then(&{:ok, Integer.to_string(&1.year)})
  end

  defp format_period_start(:hour, _timezone, %NaiveDateTime{} = datetime),
    do: {:ok, format_hour(datetime.year, datetime.month, datetime.day, datetime.hour)}

  defp format_period_start(:day, _timezone, %NaiveDateTime{} = datetime),
    do: {:ok, Date.to_iso8601(NaiveDateTime.to_date(datetime))}

  defp format_period_start(:month, _timezone, %NaiveDateTime{} = datetime),
    do: {:ok, format_month(datetime.year, datetime.month)}

  defp format_period_start(:year, _timezone, %NaiveDateTime{} = datetime),
    do: {:ok, Integer.to_string(datetime.year)}

  defp format_period_start(:hour, _timezone, %Date{} = date),
    do: {:ok, format_hour(date.year, date.month, date.day, 0)}

  defp format_period_start(:day, _timezone, %Date{} = date), do: {:ok, Date.to_iso8601(date)}

  defp format_period_start(:month, _timezone, %Date{} = date),
    do: {:ok, format_month(date.year, date.month)}

  defp format_period_start(:year, _timezone, %Date{} = date),
    do: {:ok, Integer.to_string(date.year)}

  defp format_period_start(kind, _timezone, value),
    do: {:error, {:invalid_calendar_period_start, kind, value}}

  defp valid_period_string?(:hour, value) do
    with [date, hour_and_offset] <- String.split(value, "T", parts: 2),
         {:ok, _date} <- Date.from_iso8601(date) do
      valid_hour_and_offset?(hour_and_offset)
    else
      _other -> false
    end
  end

  defp valid_period_string?(:day, value), do: match?({:ok, _date}, Date.from_iso8601(value))

  defp valid_period_string?(:month, value), do: Regex.match?(~r/^\d{4}-(0[1-9]|1[0-2])$/, value)

  defp valid_period_string?(:year, value), do: Regex.match?(~r/^\d{4}$/, value)

  defp format_hour(year, month, day, hour) do
    "#{pad4(year)}-#{pad2(month)}-#{pad2(day)}T#{pad2(hour)}"
  end

  defp format_month(year, month), do: "#{pad4(year)}-#{pad2(month)}"

  defp format_offset(offset_seconds) do
    sign = if offset_seconds < 0, do: "-", else: "+"
    absolute = abs(offset_seconds)
    "#{sign}#{pad2(div(absolute, 3600))}:#{pad2(div(rem(absolute, 3600), 60))}"
  end

  defp valid_offset?(nil, nil), do: true

  defp valid_offset?(hour, minute) do
    with {hour, ""} <- Integer.parse(hour),
         {minute, ""} <- Integer.parse(minute) do
      hour in 0..23 and minute in 0..59
    else
      _other -> false
    end
  end

  defp valid_hour_and_offset?(value) do
    case Regex.run(~r/^(\d{2})(?:[+-](\d{2}):(\d{2}))?$/, value) do
      [_, hour] ->
        valid_hour?(hour)

      [_, hour, offset_hour, offset_minute] ->
        valid_hour?(hour) and valid_offset?(offset_hour, offset_minute)

      _other ->
        false
    end
  end

  defp valid_hour?(hour) do
    case Integer.parse(hour) do
      {hour, ""} -> hour in 0..23
      _other -> false
    end
  end

  defp pad2(value), do: value |> Integer.to_string() |> String.pad_leading(2, "0")
  defp pad4(value), do: value |> Integer.to_string() |> String.pad_leading(4, "0")
end
