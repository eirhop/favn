defmodule Favn.Scheduler do
  @moduledoc """
  Public scheduler facade backed by the orchestrator runtime.
  """

  @spec reload() :: :ok | {:error, term()}
  def reload, do: FavnOrchestrator.reload_scheduler()

  @spec tick() :: :ok | {:error, term()}
  def tick, do: FavnOrchestrator.tick_scheduler()

  @spec list_scheduled_pipelines() :: [FavnOrchestrator.SchedulerEntry.t()] | {:error, term()}
  def list_scheduled_pipelines, do: FavnOrchestrator.scheduled_entries()
end
