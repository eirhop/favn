defmodule FavnView.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias FavnView.Orchestrator

  @impl true
  def start(_type, _args) do
    environment = System.get_env()

    with :ok <- FavnView.ApplicationConfig.configure(),
         :ok <- FavnView.ProductionRuntimeConfig.apply_from_env_if_configured(environment),
         :ok <- Orchestrator.configure_from_env_if_configured(environment) do
      children =
        [FavnView.Telemetry] ++
          orchestrator_connection_children() ++
          [
            pubsub_child_spec(FavnView.PubSub),
            {Task.Supervisor, name: FavnView.ReadinessTaskSupervisor},
            FavnView.Endpoint
          ]

      # See https://hexdocs.pm/elixir/Supervisor.html
      # for other strategies and supported options
      opts = [strategy: :one_for_one, name: FavnView.Supervisor]

      with {:ok, supervisor} <- Supervisor.start_link(children, opts) do
        {:ok, supervisor, %{runtime?: true}}
      end
    end
  end

  defp orchestrator_connection_children do
    case Orchestrator.target_node() do
      nil ->
        []

      target_node ->
        [
          pubsub_child_spec(FavnOrchestrator.PubSub),
          {FavnView.OrchestratorConnector, target_node: target_node}
        ]
    end
  end

  @doc false
  @spec pubsub_child_spec(atom()) :: Supervisor.child_spec()
  def pubsub_child_spec(name) do
    Supervisor.child_spec({Phoenix.PubSub, name: name}, id: name)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    FavnView.Endpoint.config_change(changed, removed)
    :ok
  end
end
