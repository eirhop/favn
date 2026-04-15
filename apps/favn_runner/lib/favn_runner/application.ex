defmodule FavnRunner.Application do
  @moduledoc false

  use Application

  alias Favn.Connection.ConfigError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry

  @impl true
  def start(_type, _args) do
    connections = load_connections_or_raise()

    children = [
      {ConnectionRegistry, name: FavnRunner.ConnectionRegistry, connections: connections},
      {Registry, keys: :unique, name: FavnRunner.ExecutionRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: FavnRunner.WorkerSupervisor},
      {FavnRunner.ManifestStore, name: FavnRunner.ManifestStore},
      {FavnRunner.Server, name: FavnRunner.Server}
    ]

    opts = [strategy: :one_for_one, name: FavnRunner.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp load_connections_or_raise do
    case Loader.load() do
      {:ok, connections} -> connections
      {:error, errors} when is_list(errors) -> raise ConfigError, errors: errors
    end
  end
end
