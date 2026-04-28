defmodule Favn.Window.Request do
  @moduledoc """
  Operator/API request for a pipeline run window before policy resolution.

  Requests describe what an operator asked for, for example the CLI value
  `month:2026-03`. The orchestrator resolves a request with
  `%Favn.Window.Policy{}` into a concrete `%Favn.Window.Anchor{}` before
  planning.

  Supported string forms are:

  - `hour:YYYY-MM-DDTHH`
  - `day:YYYY-MM-DD`
  - `month:YYYY-MM`
  - `year:YYYY`

  A request may include a timezone. When omitted, policy resolution uses the
  policy timezone or `"Etc/UTC"`.
  """

  alias Favn.Window.{Anchor, Policy, Validate}

  @type mode :: :full_load | :single
  @type kind :: Validate.kind()

  @type t :: %__MODULE__{
          mode: mode(),
          kind: kind() | nil,
          value: String.t() | nil,
          timezone: String.t() | nil
        }

  defstruct [:kind, :value, :timezone, mode: :single]

  @spec full_load() :: t()
  def full_load, do: %__MODULE__{mode: :full_load}

  @spec parse(String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  @doc """
  Parses CLI-style window input.

  ## Examples

      iex> Favn.Window.Request.parse("month:2026-03")
      {:ok, %Favn.Window.Request{mode: :single, kind: :month, value: "2026-03", timezone: nil}}
  """
  def parse(value, opts \\ []) when is_binary(value) and is_list(opts) do
    with :ok <- Validate.strict_keyword_opts(opts, [:timezone]),
         [kind_raw, date_raw] <- String.split(value, ":", parts: 2),
         {:ok, kind} <- decode_kind(kind_raw),
         timezone <- Keyword.get(opts, :timezone),
         :ok <- validate_optional_timezone(timezone),
         :ok <- validate_value(kind, date_raw) do
      {:ok, %__MODULE__{mode: :single, kind: kind, value: date_raw, timezone: timezone}}
    else
      [_only] -> {:error, {:invalid_window_request, value}}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_window_request, other}}
    end
  rescue
    ArgumentError -> {:error, {:invalid_window_request, value}}
  end

  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = request), do: validate(request)

  def from_value(input) when is_map(input) do
    mode = input |> field_value(:mode, :single) |> decode_mode()

    case mode do
      :full_load ->
        {:ok, full_load()}

      :single ->
        with {:ok, kind} <- field_value(input, :kind) |> decode_kind(),
             value <- field_value(input, :value),
             timezone <- field_value(input, :timezone),
             :ok <- validate_optional_timezone(timezone),
             :ok <- validate_value(kind, value) do
          {:ok, %__MODULE__{mode: :single, kind: kind, value: value, timezone: timezone}}
        end
    end
  end

  def from_value(other), do: {:error, {:invalid_window_request, other}}

  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{mode: :full_load} = request), do: {:ok, request}

  def validate(%__MODULE__{mode: :single, kind: kind, value: value, timezone: timezone} = request) do
    with :ok <- Validate.kind(kind),
         :ok <- validate_value(kind, value),
         :ok <- validate_optional_timezone(timezone) do
      {:ok, request}
    end
  end

  def validate(other), do: {:error, {:invalid_window_request, other}}

  @spec to_anchor(t(), String.t()) :: {:ok, Anchor.t()} | {:error, term()}
  @doc """
  Converts a single-window request into a concrete anchor window.
  """
  def to_anchor(
        %__MODULE__{mode: :single, kind: kind, value: value, timezone: timezone},
        default_timezone
      ) do
    timezone = timezone || default_timezone || "Etc/UTC"

    with :ok <- Validate.timezone(timezone),
         {:ok, start_at, end_at} <- bounds(kind, value, timezone) do
      Anchor.new(kind, start_at, end_at, timezone: timezone)
    end
  end

  def to_anchor(%__MODULE__{mode: :full_load}, _default_timezone), do: {:ok, nil}

  defp validate_optional_timezone(nil), do: :ok
  defp validate_optional_timezone(timezone), do: Validate.timezone(timezone)

  defp validate_value(:hour, value) when is_binary(value) do
    case Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}$/, value) do
      true -> :ok
      false -> {:error, {:invalid_window_value, :hour, value}}
    end
  end

  defp validate_value(:day, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, _date} -> :ok
      {:error, _reason} -> {:error, {:invalid_window_value, :day, value}}
    end
  end

  defp validate_value(:month, value) when is_binary(value) do
    case String.split(value, "-", parts: 2) do
      [year_raw, month_raw] ->
        with {year, ""} <- Integer.parse(year_raw),
             {month, ""} <- Integer.parse(month_raw),
             {:ok, _date} <- Date.new(year, month, 1) do
          :ok
        else
          _ -> {:error, {:invalid_window_value, :month, value}}
        end

      _other ->
        {:error, {:invalid_window_value, :month, value}}
    end
  end

  defp validate_value(:year, value) when is_binary(value) do
    case Regex.match?(~r/^\d{4}$/, value) do
      true -> :ok
      false -> {:error, {:invalid_window_value, :year, value}}
    end
  end

  defp validate_value(kind, value), do: {:error, {:invalid_window_value, kind, value}}

  defp bounds(:hour, value, timezone) do
    [date_raw, hour_raw] = String.split(value, "T", parts: 2)

    with {:ok, date} <- Date.from_iso8601(date_raw),
         {hour, ""} <- Integer.parse(hour_raw),
         true <- hour in 0..23,
         {:ok, start_at} <- datetime(date.year, date.month, date.day, hour, timezone) do
      {:ok, start_at, DateTime.add(start_at, 3600, :second)}
    else
      _ -> {:error, {:invalid_window_value, :hour, value}}
    end
  end

  defp bounds(:day, value, timezone) do
    with {:ok, date} <- Date.from_iso8601(value),
         {:ok, start_at} <- datetime(date.year, date.month, date.day, 0, timezone),
         {:ok, end_at} <- local_midnight(Date.add(date, 1), timezone) do
      {:ok, start_at, end_at}
    end
  end

  defp bounds(:month, value, timezone) do
    [year_raw, month_raw] = String.split(value, "-", parts: 2)

    with {year, ""} <- Integer.parse(year_raw),
         {month, ""} <- Integer.parse(month_raw),
         true <- month in 1..12,
         {:ok, start_at} <- datetime(year, month, 1, 0, timezone),
         end_at <- shift_month(start_at, 1) do
      {:ok, start_at, end_at}
    else
      _ -> {:error, {:invalid_window_value, :month, value}}
    end
  end

  defp bounds(:year, value, timezone) do
    with {year, ""} <- Integer.parse(value),
         {:ok, start_at} <- datetime(year, 1, 1, 0, timezone),
         {:ok, end_at} <- datetime(year + 1, 1, 1, 0, timezone) do
      {:ok, start_at, end_at}
    else
      _ -> {:error, {:invalid_window_value, :year, value}}
    end
  end

  defp datetime(year, month, day, hour, timezone) do
    with {:ok, date} <- Date.new(year, month, day),
         {:ok, naive} <- NaiveDateTime.new(date, Time.new!(hour, 0, 0)) do
      {:ok, DateTime.from_naive!(naive, timezone, Favn.Timezone.database!())}
    end
  rescue
    ArgumentError -> {:error, {:invalid_timezone, timezone}}
  end

  defp shift_month(%DateTime{} = datetime, count) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    year = div(total, 12)
    month = rem(total, 12) + 1
    {:ok, shifted} = datetime(year, month, 1, 0, datetime.time_zone)
    shifted
  end

  defp local_midnight(%Date{} = date, timezone) do
    datetime(date.year, date.month, date.day, 0, timezone)
  end

  defp field_value(value, field, default \\ nil) when is_map(value) do
    Map.get(value, field, Map.get(value, Atom.to_string(field), default))
  end

  defp decode_mode(value) when value in [:single, "single"], do: :single
  defp decode_mode(value) when value in [:full_load, "full_load"], do: :full_load

  defp decode_mode(value),
    do: raise(ArgumentError, "invalid window request mode #{inspect(value)}")

  defp decode_kind(value) when is_atom(value), do: Policy.normalize_kind(value)
  defp decode_kind("hour"), do: {:ok, :hour}
  defp decode_kind("hourly"), do: {:ok, :hour}
  defp decode_kind("day"), do: {:ok, :day}
  defp decode_kind("daily"), do: {:ok, :day}
  defp decode_kind("month"), do: {:ok, :month}
  defp decode_kind("monthly"), do: {:ok, :month}
  defp decode_kind("year"), do: {:ok, :year}
  defp decode_kind("yearly"), do: {:ok, :year}
  defp decode_kind(value), do: {:error, {:invalid_window_kind, value}}
end
