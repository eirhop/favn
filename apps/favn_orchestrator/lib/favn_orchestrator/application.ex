defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.BackfillDispatcher
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.ExecutionAdmission.Coordinator, as: AdmissionCoordinator
  alias FavnOrchestrator.LocalDevBootstrap
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.ProductionRuntimeConfig
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunRecovery
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.PersistenceRuntime, as: PersistenceSchedulerRuntime

  @impl true
  def start(_type, _args) do
    with :ok <- ProductionRuntimeConfig.apply_from_env_if_configured(),
         _timezone_database <- Favn.Timezone.database!(),
         :ok <- APIConfig.validate(),
         runtime_config <- RuntimeConfig.from_app_env(),
         persistence_runtime <- PersistenceRuntime.from_app_env!(),
         {:ok, persistence_children} <- Persistence.child_specs(persistence_runtime) do
      OperationalEvents.emit(
        :orchestrator_starting,
        %{persistence_child_count: length(persistence_children)},
        %{
          persistence_backend: persistence_runtime.backend,
          scheduler_enabled?: scheduler_enabled?(),
          api_enabled?: api_enabled?()
        }
      )

      children =
        [{RuntimeConfig, runtime_config}] ++
          [{PersistenceRuntime, persistence_runtime}] ++
          persistence_children ++
          [{ManifestIndexCache, []}] ++
          local_dev_bootstrap_children() ++
          [
            {AuthStore, []},
            {Phoenix.PubSub, name: pubsub_name()},
            {AdmissionCoordinator, []},
            {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
            {Task.Supervisor, name: FavnOrchestrator.RunManagerTaskSupervisor},
            {RunManager, []}
          ] ++
          [{BackfillDispatcher, []}] ++
          [{RunRecovery, []}, {BoundedDispatcher, []}] ++ scheduler_children() ++ api_children()

      with {:ok, supervisor} <-
             Supervisor.start_link(children,
               strategy: :one_for_one,
               name: FavnOrchestrator.Supervisor
             ),
           :ok <- Auth.bootstrap_configured_actor() do
        OperationalEvents.emit(:orchestrator_started, %{}, %{})
        {:ok, supervisor}
      end
    end
  end

  defp scheduler_children do
    scheduler_opts = Application.get_env(:favn_orchestrator, :scheduler, [])

    if Keyword.get(scheduler_opts, :enabled, false) do
      [{PersistenceSchedulerRuntime, scheduler_opts}]
    else
      OperationalEvents.emit(:scheduler_disabled, %{}, %{})
      []
    end
  end

  defp local_dev_bootstrap_children do
    if Application.get_env(:favn_orchestrator, :local_dev_mode, false) do
      [{LocalDevBootstrap, []}]
    else
      []
    end
  end

  defp scheduler_enabled? do
    :favn_orchestrator
    |> Application.get_env(:scheduler, [])
    |> Keyword.get(:enabled, false)
  end

  defp pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end

  defp api_children do
    api_opts = Application.get_env(:favn_orchestrator, :api_server, [])

    if Keyword.get(api_opts, :enabled, false) do
      case APIConfig.server_options(api_opts) do
        {:ok, server_options} ->
          [{Bandit, [scheme: :http, plug: FavnOrchestrator.API.Router] ++ server_options}]

        {:error, reason} ->
          raise ArgumentError, "invalid orchestrator API server config: #{inspect(reason)}"
      end
    else
      []
    end
  end

  defp api_enabled? do
    :favn_orchestrator
    |> Application.get_env(:api_server, [])
    |> Keyword.get(:enabled, false)
  end
end
