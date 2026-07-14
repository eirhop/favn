defmodule FavnOrchestrator.WindowSummary do
  @moduledoc """
  Pure normalization of persisted run and backfill window data for read models.

  Storage lookup remains at read-model edges; this module only translates values
  that have already crossed the storage boundary.
  """

  alias Favn.Window.Anchor
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState

  @type t :: %{
          required(:key) => String.t() | nil,
          required(:label) => String.t() | nil,
          required(:kind) => atom() | nil,
          required(:start_at) => DateTime.t() | nil,
          required(:end_at) => DateTime.t() | nil,
          required(:timezone) => String.t() | nil
        }

  @doc "Returns window metadata already embedded in a run snapshot."
  @spec from_run(RunState.t()) :: t() | nil
  def from_run(%RunState{} = run) do
    from_metadata(run.metadata) || from_params(run.params)
  end

  @doc "Converts a persisted backfill window to the public window shape."
  @spec from_backfill(BackfillWindow.t()) :: t()
  def from_backfill(%BackfillWindow{} = window) do
    %{
      key: window.window_key,
      label: label(window.window_kind, window.window_start_at),
      kind: window.window_kind,
      start_at: window.window_start_at,
      end_at: window.window_end_at,
      timezone: window.timezone
    }
  end

  @doc "Normalizes an arbitrary map into the public window shape."
  @spec public(map() | nil) :: t()
  def public(nil), do: public(%{})

  def public(window) when is_map(window) do
    kind = get(window, :kind)
    start_at = get(window, :start_at)

    %{
      key: get(window, :key) || get(window, :id),
      label: get(window, :label) || label(kind, start_at),
      kind: kind,
      start_at: start_at,
      end_at: get(window, :end_at),
      timezone: get(window, :timezone)
    }
  end

  @doc "Returns whether a normalized window has no usable identity or bounds."
  @spec empty?(t() | map() | nil) :: boolean()
  def empty?(nil), do: true

  def empty?(window) when is_map(window) do
    is_nil(Map.get(window, :key)) and is_nil(Map.get(window, :start_at)) and
      is_nil(Map.get(window, :end_at))
  end

  def empty?(_window), do: false

  defp from_metadata(metadata) when is_map(metadata) do
    metadata
    |> get(:pipeline_context, %{})
    |> from_anchor_context()
    |> then(&(&1 || selected_window(metadata)))
  end

  defp from_metadata(_metadata), do: nil

  defp from_anchor_context(%{anchor_window: %Anchor{} = anchor}), do: from_anchor(anchor)
  defp from_anchor_context(%{"anchor_window" => %Anchor{} = anchor}), do: from_anchor(anchor)
  defp from_anchor_context(_value), do: nil

  defp from_anchor(%Anchor{} = anchor) do
    %{
      key: encoded_key(anchor),
      label: label(anchor.kind, anchor.start_at),
      kind: anchor.kind,
      start_at: anchor.start_at,
      end_at: anchor.end_at,
      timezone: anchor.timezone
    }
  end

  defp selected_window(metadata) do
    from_value(get(metadata, :selected_window) || get(metadata, :window))
  end

  defp from_params(params) when is_map(params), do: params |> get(:window) |> from_value()
  defp from_params(_params), do: nil

  defp from_value(value) when is_map(value), do: public(value)

  defp from_value(value) when is_binary(value) do
    %{key: value, label: value, kind: nil, start_at: nil, end_at: nil, timezone: nil}
  end

  defp from_value(_value), do: nil

  defp encoded_key(%Anchor{kind: kind, key: key}) when is_map(key) do
    case {Map.get(key, :timezone), Map.get(key, :start_at_us)} do
      {timezone, start_at_us} when is_binary(timezone) and is_integer(start_at_us) ->
        start_at = DateTime.from_unix!(start_at_us, :microsecond)
        "#{kind}:#{timezone}:#{DateTime.to_iso8601(start_at)}"

      _other ->
        nil
    end
  end

  defp label(kind, %DateTime{} = start_at) do
    case kind do
      :hour -> Calendar.strftime(start_at, "%b %-d %H:00")
      :day -> Calendar.strftime(start_at, "%b %-d")
      :month -> Calendar.strftime(start_at, "%b %Y")
      :year -> Calendar.strftime(start_at, "%Y")
      _other -> nil
    end
  end

  defp label(_kind, _start_at), do: nil

  defp get(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key), default)
  end
end
