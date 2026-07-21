defmodule FavnRunner.OperationalEvents do
  @moduledoc """
  Emits bounded runner lifecycle telemetry and structured operational logs.
  """

  require Logger

  @doc "Emits one runner-owned operational event with bounded metadata."
  @spec emit(atom(), map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{})
      when is_atom(event) and is_map(measurements) and is_map(metadata) do
    Logger.info(
      "favn.runner.#{event} measurements=#{inspect(measurements)} metadata=#{inspect(metadata)}"
    )

    :telemetry.execute([:favn, :runner, event], measurements, metadata)
    :ok
  end
end
