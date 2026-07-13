defmodule FavnOrchestrator.Application do
  @moduledoc false

  use Application

  alias FavnOrchestrator.API.Config, as: APIConfig
  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.ExecutionAdmission.Coordinator, as: AdmissionCoordinator
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ProductionRuntimeConfig
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunRecovery
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.Storage

  @impl true
  def start(_type, _args) do
    with :ok <- ProductionRuntimeConfig.apply_from_env_if_configured(),
         _timezone_database <- Favn.Timezone.database!(),
         :ok <- APIConfig.validate(),
         runtime_config <- RuntimeConfig.from_app_env(),
         {:ok, storage_children} <- Storage.child_specs(runtime_config) do
      OperationalEvents.emit(
        :orchestrator_starting,
        %{storage_child_count: length(storage_children)},
        %{
          storage_adapter: runtime_config.storage_adapter,
          scheduler_enabled?: scheduler_enabled?(),
          api_enabled?: api_enabled?()
        }
      )

      children =
        [{RuntimeConfig, runtime_config}] ++
          storage_children ++
          [
            {AuthStore, []},
            {Phoenix.PubSub, name: pubsub_name()},
            {AdmissionCoordinator, []},
            {RunRecovery, []},
            {DynamicSupervisor, strategy: :one_for_one, name: FavnOrchestrator.RunSupervisor},
            {RunManager, []},
            {BoundedDispatcher, []}
          ] ++ scheduler_children() ++ api_children()

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
      [{SchedulerRuntime, scheduler_opts}]
    else
      OperationalEvents.emit(:scheduler_disabled, %{}, %{})
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
      [
        {Bandit,
         [scheme: :http, plug: FavnOrchestrator.API.Router] ++ api_server_options(api_opts)}
      ]
    else
      []
    end
  end

  defp api_enabled? do
    :favn_orchestrator
    |> Application.get_env(:api_server, [])
    |> Keyword.get(:enabled, false)
  end

  defp api_server_options(api_opts) do
    case APIConfig.bind_ip(api_opts) do
      {:ok, bind_ip} ->
        bind_ip

      {:error, reason} ->
        raise ArgumentError, "invalid orchestrator API bind config: #{inspect(reason)}"
    end
    |> case do
      nil -> [port: Keyword.get(api_opts, :port, 4101)]
      bind_ip -> [port: Keyword.get(api_opts, :port, 4101), ip: bind_ip]
    end
  end
end
