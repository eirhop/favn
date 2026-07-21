defmodule FavnView.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    with :ok <- ensure_runtime_config() do
      start_storybook()

      children = [
        FavnView.Telemetry,
        {Phoenix.PubSub, name: FavnView.PubSub},
        {Task.Supervisor, name: FavnView.ReadinessTaskSupervisor},
        # Start to serve requests, typically the last entry
        FavnView.Endpoint
      ]

      # See https://hexdocs.pm/elixir/Supervisor.html
      # for other strategies and supported options
      opts = [strategy: :one_for_one, name: FavnView.Supervisor]

      with {:ok, supervisor} <- Supervisor.start_link(children, opts) do
        {:ok, supervisor, %{runtime?: true, drain: control_plane_drain_callback()}}
      end
    end
  end

  @impl true
  def prep_stop(%{runtime?: true, drain: drain} = state) when is_function(drain, 0) do
    _ = drain.()
    state
  end

  def prep_stop(state), do: state

  defp control_plane_drain_callback do
    if Application.get_env(:favn_view, :production_runtime_config, false) do
      &FavnOrchestrator.drain/0
    else
      fn -> :ok end
    end
  end

  defp ensure_runtime_config do
    if Application.get_env(:favn_view, :production_runtime_config, false) do
      FavnOrchestrator.ControlPlaneRuntimeConfig.ensure_applied()
    else
      :ok
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
