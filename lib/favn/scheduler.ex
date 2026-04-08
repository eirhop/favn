defmodule Favn.Scheduler do
  @moduledoc """
  Scheduler runtime façade.
  """

  @spec reload() :: :ok | {:error, term()}
  def reload, do: Favn.Scheduler.Runtime.reload()

  @spec tick() :: :ok
  def tick, do: Favn.Scheduler.Runtime.tick()

  @spec list_scheduled_pipelines() :: [map()]
  def list_scheduled_pipelines, do: Favn.Scheduler.Runtime.scheduled()
end
