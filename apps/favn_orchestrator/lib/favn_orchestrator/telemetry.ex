defmodule FavnOrchestrator.Telemetry do
  @moduledoc """
  Standard telemetry event boundary for orchestrator operational events.

  Event names are emitted as `[:favn, :orchestrator, event]`, where `event` is a
  stable operational event atom owned by the orchestrator.
  """

  @type event :: atom()
  @type measurements :: map()
  @type metadata :: map()

  @doc "Emits one orchestrator telemetry event."
  @spec emit(event(), measurements(), metadata()) :: :ok
  def emit(event, measurements, metadata)
      when is_atom(event) and is_map(measurements) and is_map(metadata) do
    :telemetry.execute([:favn, :orchestrator, event], measurements, metadata)
    :ok
  end
end
