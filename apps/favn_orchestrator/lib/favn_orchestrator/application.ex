defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.API.IdempotencyStore
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @impl true
  def start(_type, _args) do
    with :ok <- APIConfig.validate(),
         {:ok, storage_children} <- Storage.child_specs() do
      children =
        storage_children ++
          [
            {IdempotencyStore, []},
            {AuthStore, []},
            {Phoenix.PubSub, name: pubsub_name()},
            {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
            {RunManager, []}
          ] ++ scheduler_children() ++ api_children()

      with {:ok, supervisor} <-
             Supervisor.start_link(children,
               strategy: :one_for_one,
               name: FavnOrchestrator.Supervisor
             ),
           :ok <- Auth.bootstrap_admin() do
        {:ok, supervisor}
      end
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

  defp api_children do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      [
        {Plug.Cowboy,
         scheme: :http,
         plug: FavnOrchestrator.API.Router,
         options: [port: Keyword.get(api_opts, :port, 4101)]}
      ]
    else
      []
    end
  end
end
