defmodule Favn.SQL.Observability do
  @moduledoc false

  require Logger

  @event_prefix [:favn, :sql]

  @spec emit([atom()], map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{})
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    telemetry_event = @event_prefix ++ event
    maybe_emit_telemetry(telemetry_event, measurements, metadata)

    Logger.debug(fn ->
      "#{format_event(event)} #{inspect(%{measurements: measurements, metadata: metadata})}"
    end)

    :ok
  end

  defp maybe_emit_telemetry(event, measurements, metadata) do
    if Code.ensure_loaded?(:telemetry) do
      apply(:telemetry, :execute, [event, measurements, metadata])
    end

    :ok
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp format_event(event) do
    event
    |> Enum.map(&Atom.to_string/1)
    |> Enum.join(".")
    |> then(&"favn.sql.#{&1}")
  end
end
