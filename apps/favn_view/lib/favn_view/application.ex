defmodule FavnView.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    with :ok <- FavnView.ProductionRuntimeConfig.apply_from_env_if_configured() do
      start_storybook()

      children = [
        FavnView.Telemetry,
        {Phoenix.PubSub, name: FavnView.PubSub},
        FavnView.Auth.BrowserSessionStore,
        {Task.Supervisor, name: FavnView.ReadinessTaskSupervisor},
        # Start to serve requests, typically the last entry
        FavnView.Endpoint
      ]

      # See https://hexdocs.pm/elixir/Supervisor.html
      # for other strategies and supported options
      opts = [strategy: :one_for_one, name: FavnView.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    FavnView.Endpoint.config_change(changed, removed)
    :ok
  end

  defp start_storybook do
    if Application.get_env(:favn_view, :dev_routes, false) do
      {:ok, _apps} = Application.ensure_all_started(:phoenix_storybook)
    end

    :ok
  end
end
