defmodule FavnRunner.Application do
  @moduledoc false

  use Application

  alias Favn.Connection.ConfigError
  alias Favn.Connection.Loader
  alias Favn.Connection.Registry, as: ConnectionRegistry
  alias FavnRunner.{ExtensionSupervisor, PluginLoader}
  alias FavnRunner.Lifecycle
  alias FavnRunner.ProductionRuntimeConfig
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.RuntimeStarter
  alias FavnRunner.Shutdown

  @impl true
  def start(_type, _args) do
    environment = System.get_env()
    :ok = configure_log_level_or_raise(environment)
    :ok = apply_production_runtime_config_or_raise(environment)
    :ok = verify_release_or_raise(environment)
    connections = load_connections_or_raise()
    plugin_children = load_plugin_children_or_raise()

    shutdown_drain_timeout_ms =
      Application.get_env(:favn_runner, :shutdown_drain_timeout_ms, 120_000)

    children =
      [
        {Lifecycle, shutdown_drain_timeout_ms: shutdown_drain_timeout_ms},
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
         retention: Application.get_env(:favn_runner, :execution_retention, [])},
        {RuntimeStarter, []}
      ]

    opts = [strategy: :one_for_all, name: FavnRunner.Supervisor]

    with {:ok, supervisor} <- Supervisor.start_link(children, opts) do
      {:ok, supervisor, %{runtime?: true}}
    end
  end

  @impl true
  def prep_stop(%{runtime?: true} = state) do
    _ = Shutdown.drain()
    state
  end

  def prep_stop(state), do: state

  defp configure_log_level_or_raise(environment) do
    case Favn.LogLevel.configure_from_env(environment) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid FAVN_LOG_LEVEL: #{inspect(reason)}"
    end
  end

  defp apply_production_runtime_config_or_raise(environment) do
    case ProductionRuntimeConfig.apply_from_env_if_configured(environment) do
      :ok ->
        :ok

      {:error, reason} ->
        raise ArgumentError, "invalid runner production config: #{inspect(reason)}"
    end
  end

  defp verify_release_or_raise(environment) do
    case ReleaseVerifier.verify_startup(environment) do
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
