defmodule FavnRunner.Application do
  @moduledoc false

  use Application

  alias Favn.Connection.ConfigError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias FavnRunner.Plugin
  alias FavnRunner.ProductionRuntimeConfig

  @impl true
  def start(_type, _args) do
    :ok = apply_production_runtime_config_or_raise()
    connections = load_connections_or_raise()
    plugin_children = load_plugin_children_or_raise()

    children =
      plugin_children ++
        [
          {ConnectionRegistry, name: FavnRunner.ConnectionRegistry, connections: connections},
          {Registry, keys: :unique, name: FavnRunner.ExecutionRegistry},
          {DynamicSupervisor, strategy: :one_for_one, name: FavnRunner.WorkerSupervisor},
          {FavnRunner.ManifestStore, name: FavnRunner.ManifestStore},
          {FavnRunner.Server, name: FavnRunner.Server}
        ]

    opts = [strategy: :one_for_one, name: FavnRunner.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp apply_production_runtime_config_or_raise do
    case ProductionRuntimeConfig.apply_from_env() do
      :ok ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "invalid runner production config: #{inspect(reason)}"
    end
  end

  defp load_connections_or_raise do
    case Loader.load() do
      {:ok, connections} -> connections
      {:error, errors} when is_list(errors) -> raise ConfigError, errors: errors
    end
  end

  defp load_plugin_children_or_raise do
    entries = Application.get_env(:favn, :runner_plugins, [])

    case Plugin.normalize_config(entries) do
      {:ok, plugins} ->
        Enum.flat_map(plugins, fn {plugin, opts} ->
          case plugin.child_specs(opts) do
            specs when is_list(specs) ->
              specs

            other ->
              raise ArgumentError,
                    "invalid plugin child_specs from #{inspect(plugin)}: #{inspect(other)}"
          end
        end)

      {:error, reason} ->
        raise ArgumentError, "invalid runner plugin config: #{inspect(reason)}"
    end
  end
end
