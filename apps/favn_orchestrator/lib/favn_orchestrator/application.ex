defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @impl true
  def start(_type, _args) do
    with {:ok, storage_children} <- Storage.child_specs() do
      children =
        storage_children ++
          [
            {Phoenix.PubSub, name: pubsub_name()},
            {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
            {RunManager, []}
          ] ++ scheduler_children()

      Supervisor.start_link(children, strategy: :one_for_one, name: FavnOrchestrator.Supervisor)
    end
  end

  defp scheduler_children do
    scheduler_opts = Application.get_env(:favn_orchestrator, :scheduler, [])

    if Keyword.get(scheduler_opts, :enabled, false) do
      [{SchedulerRuntime, scheduler_opts}]
    else
      []
    end
  end

  defp pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end
end
