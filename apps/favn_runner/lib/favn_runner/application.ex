defmodule FavnRunner.Application do
  @moduledoc false

  use Application

  alias Favn.Connection.ConfigError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias FavnRunner.{ExtensionSupervisor, PluginLoader}
  alias FavnRunner.ProductionRuntimeConfig
  alias FavnRunner.ReleaseVerifier

  @impl true
  def start(_type, _args) do
    :ok = apply_production_runtime_config_or_raise()
    :ok = verify_release_or_raise()
    connections = load_connections_or_raise()
    plugin_children = load_plugin_children_or_raise()

    children =
      [
        {ExtensionSupervisor, children: plugin_children},
        {ConnectionRegistry, name: FavnRunner.ConnectionRegistry, connections: connections},
        {Registry, keys: :unique, name: FavnRunner.ExecutionRegistry},
        {DynamicSupervisor, strategy: :one_for_one, name: FavnRunner.WorkerSupervisor},
        {FavnRunner.ManifestStore,
         Keyword.put(
           Application.get_env(:favn_runner, :manifest_cache, []),
           :name,
           FavnRunner.ManifestStore
         )},
        {FavnRunner.Server,
         name: FavnRunner.Server,
         admission: Application.get_env(:favn_runner, :admission, []),
         retention: Application.get_env(:favn_runner, :execution_retention, [])}
      ]

    opts = [strategy: :rest_for_one, name: FavnRunner.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp apply_production_runtime_config_or_raise do
    case ProductionRuntimeConfig.apply_from_env_if_configured() do
      :ok ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "invalid runner production config: #{inspect(reason)}"
    end
  end

  defp verify_release_or_raise do
    case ReleaseVerifier.verify_startup() do
      :ok ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "runner release verification failed: #{inspect(reason)}"
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

    result =
      case ReleaseVerifier.prepared_plugin_children() do
        {:ok, children} -> {:ok, children}
        :not_prepared -> PluginLoader.load(entries)
      end

    case result do
      {:ok, children} -> children
      {:error, reason} -> raise ArgumentError, PluginLoader.format_error(reason)
    end
  end
end
