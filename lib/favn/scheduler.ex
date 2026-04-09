defmodule Favn.Scheduler do
  @moduledoc """
  Scheduler runtime façade.
  """

  alias Favn.Scheduler.Runtime

  @spec reload() :: :ok | {:error, term()}
  def reload, do: Runtime.reload()

  @spec tick() :: :ok
  def tick, do: Runtime.tick()

  @spec list_scheduled_pipelines() :: [map()]
  def list_scheduled_pipelines, do: Runtime.scheduled()
end
