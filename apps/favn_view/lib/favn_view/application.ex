defmodule FavnView.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      FavnViewWeb.Telemetry,
      {Phoenix.PubSub, name: FavnView.PubSub},
      FavnView.Endpoint
    ]

    opts = [strategy: :one_for_one, name: FavnView.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    FavnView.Endpoint.config_change(changed, removed)
    :ok
  end
end
