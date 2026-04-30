defmodule Favn.Backfill.RangeResolver do
  @moduledoc """
  Resolves operational backfill range requests into concrete window anchors.

  This is the pure planning step for operational backfills. It validates an
  explicit or relative `Favn.Backfill.RangeRequest` and returns ordered
  `Favn.Window.Anchor` values plus encoded window keys. The orchestrator uses
  this result to create parent and child runs, but this module has no storage or
  runtime side effects.
  """

  alias Favn.Backfill.RangeRequest
  alias Favn.Window.{Anchor, Key, Request, Validate}

  @type resolved :: %{
          kind: Validate.kind(),
          timezone: String.t(),
          anchors: [Anchor.t()],
          window_keys: [Key.t()],
          range_start_at: DateTime.t(),
          range_end_at: DateTime.t(),
          requested_count: pos_integer(),
          reference: term()
        }

  @doc """
  Resolves a backfill range request into anchor windows and window keys.

  The result is ordered by window start and includes the inclusive request count,
  concrete range bounds, and the reference datetime used for relative requests.
  """
  @spec resolve(RangeRequest.t() | map() | keyword()) :: {:ok, resolved()} | {:error, term()}
  def resolve(value) do
    with {:ok, request} <- RangeRequest.from_value(value) do
      resolve_request(request)
    end
  end

  defp resolve_request(%RangeRequest{mode: :explicit} = request) do
    with {:ok, from_anchor} <- request_anchor(request.kind, request.from, request.timezone),
         {:ok, to_anchor} <- request_anchor(request.kind, request.to, request.timezone),
         :ok <- validate_order(from_anchor.start_at, to_anchor.end_at),
         {:ok, anchors} <-
           Anchor.expand_range(request.kind, from_anchor.start_at, to_anchor.end_at,
             timezone: request.timezone
           ) do
      build_resolved_range(request.kind, request.timezone, anchors, length(anchors), nil)
    end
  end

  defp resolve_request(%RangeRequest{mode: :relative_last, last: {count, kind}} = request) do
    reference = request.relative_to || coverage_until(request.baseline)

    with {:ok, end_at} <- exclusive_end_boundary(reference, kind, request.timezone),
         {:ok, start_at} <- shift_kind(end_at, kind, -count),
         {:ok, anchors} <- Anchor.expand_range(kind, start_at, end_at, timezone: request.timezone) do
      build_resolved_range(kind, request.timezone, anchors, count, reference)
    end
  end

  defp request_anchor(kind, value, timezone) do
    %Request{kind: kind, value: value, timezone: timezone}
    |> Request.to_anchor(timezone)
  end

  defp build_resolved_range(kind, timezone, anchors, requested_count, reference) do
    {:ok,
     %{
       kind: kind,
       timezone: timezone,
       anchors: anchors,
       window_keys: Enum.map(anchors, &Key.from_window/1),
       range_start_at: anchors |> List.first() |> Map.fetch!(:start_at),
       range_end_at: anchors |> List.last() |> Map.fetch!(:end_at),
       requested_count: requested_count,
       reference: reference
     }}
  end

  defp validate_order(%DateTime{} = start_at, %DateTime{} = end_at) do
    case DateTime.compare(start_at, end_at) do
      :lt -> :ok
      _other -> {:error, :invalid_backfill_range_bounds}
    end
  end

  defp exclusive_end_boundary(%DateTime{} = reference, kind, timezone) do
    local = DateTime.shift_zone!(reference, timezone, Favn.Timezone.database!())

    with {:ok, floor} <- floor_boundary(local, kind, timezone) do
      if DateTime.compare(local, floor) == :eq do
        {:ok, floor}
      else
        shift_kind(floor, kind, 1)
      end
    end
  rescue
    ArgumentError -> {:error, {:invalid_timezone, timezone}}
  end

  defp floor_boundary(local, :hour, _timezone),
    do: {:ok, %{local | minute: 0, second: 0, microsecond: {0, 0}}}

  defp floor_boundary(local, :day, timezone),
    do: local_midnight(local.year, local.month, local.day, timezone)

  defp floor_boundary(local, :month, timezone),
    do: local_midnight(local.year, local.month, 1, timezone)

  defp floor_boundary(local, :year, timezone), do: local_midnight(local.year, 1, 1, timezone)

  defp shift_kind(datetime, :hour, count),
    do: {:ok, DateTime.add(datetime, count * 3600, :second)}

  defp shift_kind(datetime, :day, count), do: shift_date(datetime, count, :day)
  defp shift_kind(datetime, :month, count), do: shift_date(datetime, count, :month)
  defp shift_kind(datetime, :year, count), do: shift_date(datetime, count, :year)

  defp shift_date(%DateTime{} = datetime, count, :day) do
    date = datetime |> DateTime.to_date() |> Date.add(count)
    local_midnight(date.year, date.month, date.day, datetime.time_zone)
  end

  defp shift_date(%DateTime{} = datetime, count, :month) do
    date = DateTime.to_date(datetime)
    total = date.year * 12 + (date.month - 1) + count
    local_midnight(div(total, 12), rem(total, 12) + 1, 1, datetime.time_zone)
  end

  defp shift_date(%DateTime{} = datetime, count, :year) do
    date = DateTime.to_date(datetime)
    local_midnight(date.year + count, 1, 1, datetime.time_zone)
  end

  defp local_midnight(year, month, day, timezone) do
    with {:ok, date} <- Date.new(year, month, day),
         {:ok, naive} <- NaiveDateTime.new(date, ~T[00:00:00]) do
      {:ok, DateTime.from_naive!(naive, timezone, Favn.Timezone.database!())}
    end
  rescue
    ArgumentError -> {:error, {:invalid_timezone, timezone}}
  end

  defp coverage_until(nil), do: nil

  defp coverage_until(value) when is_map(value),
    do: parse_datetime(Map.get(value, :coverage_until, Map.get(value, "coverage_until")))

  defp parse_datetime(%DateTime{} = value), do: value

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp parse_datetime(_value), do: nil
end
