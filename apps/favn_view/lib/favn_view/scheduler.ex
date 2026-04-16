defmodule FavnView.Scheduler do
  @moduledoc """
  Thin view-side context over orchestrator scheduler APIs.
  """

  alias FavnOrchestrator

  @spec scheduled_entries() :: [FavnOrchestrator.SchedulerEntry.t()] | {:error, term()}
  def scheduled_entries, do: FavnOrchestrator.scheduled_entries()

  @spec reload() :: :ok | {:error, term()}
  def reload, do: FavnOrchestrator.reload_scheduler()

  @spec tick() :: :ok | {:error, term()}
  def tick, do: FavnOrchestrator.tick_scheduler()
end
