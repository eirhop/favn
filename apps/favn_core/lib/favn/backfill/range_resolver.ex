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
  alias Favn.TimePeriod
  alias Favn.Window.{Anchor, Key, Validate}

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
         {:ok, start_at} <- TimePeriod.shift(end_at, kind, -count),
         {:ok, anchors} <- Anchor.expand_range(kind, start_at, end_at, timezone: request.timezone) do
      build_resolved_range(kind, request.timezone, anchors, count, reference)
    end
  end

  defp request_anchor(kind, value, timezone) do
    with {:ok, period} <- TimePeriod.bounds(kind, value, timezone) do
      Anchor.new(kind, period.start_at, period.end_at, timezone: timezone)
    end
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
    with {:ok, floor} <- TimePeriod.floor(reference, kind, timezone) do
      if DateTime.compare(reference, floor) == :eq do
        {:ok, floor}
      else
        TimePeriod.shift(floor, kind, 1)
      end
    end
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
