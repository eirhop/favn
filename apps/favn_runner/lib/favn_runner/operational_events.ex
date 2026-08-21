defmodule FavnRunner.OperationalEvents do
  @moduledoc """
  Emits bounded runner lifecycle telemetry and structured operational logs.
  """

  require Logger

  @doc "Emits one runner-owned operational event with bounded metadata."
  @spec emit(atom(), map(), map(), keyword()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{}, opts \\ [])
      when is_atom(event) and is_map(measurements) and is_map(metadata) and is_list(opts) do
    if Keyword.get(opts, :log?, true) do
      Logger.log(
        Keyword.get(opts, :level, :info),
        "favn.runner.#{event} measurements=#{inspect(measurements)} metadata=#{inspect(metadata)}"
      )
    end

    :telemetry.execute([:favn, :runner, event], measurements, metadata)
    :ok
  end
end
