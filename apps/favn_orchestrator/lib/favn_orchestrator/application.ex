defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.ActiveManifestReconciler
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.BackfillDispatcher
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.ControlPlaneRuntimeConfig
  alias FavnOrchestrator.ExecutionAdmission.Coordinator, as: AdmissionCoordinator
  alias FavnOrchestrator.LocalDevBootstrap
  alias FavnOrchestrator.Lifecycle
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.RebuildDispatcher
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunRecovery
  alias FavnOrchestrator.RunnerHealth
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.RuntimeStarter
  alias FavnOrchestrator.Shutdown
  alias FavnOrchestrator.Scheduler.PersistenceRuntime, as: PersistenceSchedulerRuntime

  @impl true
  def start(_type, _args) do
    environment = System.get_env()
    :ok = configure_log_level_or_raise(environment)

    if Application.get_env(:favn_orchestrator, :start_runtime, true) do
      start_runtime(environment)
    else
      timeout_ms = Application.get_env(:favn_orchestrator, :shutdown_drain_timeout_ms, 120_000)

      with {:ok, supervisor} <-
             Supervisor.start_link(
               [
                 {Lifecycle, shutdown_drain_timeout_ms: timeout_ms},
                 {RuntimeStarter, runtime?: false}
               ],
               strategy: :one_for_all,
               name: FavnOrchestrator.Supervisor
             ) do
        {:ok, supervisor, %{runtime?: false}}
      end
    end
  end

  defp start_runtime(environment) do
    with :ok <- ControlPlaneRuntimeConfig.apply_from_env_if_configured(environment),
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
          scheduler_enabled?: scheduler_enabled?(runtime_config),
          api_enabled?: api_enabled?(runtime_config)
        }
      )

      children =
        [{RuntimeConfig, runtime_config}] ++
          [{Lifecycle, shutdown_drain_timeout_ms: runtime_config.shutdown_drain_timeout_ms}] ++
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
            {RunManager, []},
            {FavnOrchestrator.ResourceRecovery, []}
          ] ++
          [{BackfillDispatcher, []}, {RebuildDispatcher, []}] ++
          [
            {RunRecovery, []},
            {RunnerHealth, []},
            {ActiveManifestReconciler, []},
            {BoundedDispatcher, []}
          ] ++
          scheduler_children(runtime_config) ++
          api_children(runtime_config) ++ [{RuntimeStarter, runtime?: true}]

      with {:ok, supervisor} <-
             Supervisor.start_link(children,
               strategy: :one_for_all,
               name: FavnOrchestrator.Supervisor
             ) do
        {:ok, supervisor, %{runtime?: true}}
      end
    end
  end

  @impl true
  def prep_stop(%{runtime?: true} = state) do
    _ = Shutdown.drain()
    state
  end

  def prep_stop(%{runtime?: false} = state) do
    _ = Lifecycle.drain()
    _ = Lifecycle.stop()
    state
  end

  def prep_stop(state), do: state

  defp configure_log_level_or_raise(environment) do
    case Favn.LogLevel.configure_from_env(environment) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid FAVN_LOG_LEVEL: #{inspect(reason)}"
    end
  end

  defp scheduler_children(runtime_config) do
    scheduler_opts = runtime_config.scheduler

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

  defp scheduler_enabled?(runtime_config),
    do: Keyword.get(runtime_config.scheduler, :enabled, false)

  defp pubsub_name do
    Application.get_env(:favn_orchestrator, :pubsub_name, FavnOrchestrator.PubSub)
  end

  defp api_children(runtime_config) do
    api_opts = runtime_config.api_server

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

  defp api_enabled?(runtime_config),
    do: Keyword.get(runtime_config.api_server, :enabled, false)
end
