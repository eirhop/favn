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
         retention: Application.get_env(:favn_runner, :execution_retention, [])}
      ] ++
        runner_agent_children(environment) ++
        [
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

  defp runner_agent_children(environment) do
    case Map.get(environment, "FAVN_CONTROL_PLANE_NODE") do
      nil ->
        []

      "" ->
        []

      control_plane_node ->
        runner_pool =
          Map.get(environment, "FAVN_RUNNER_POOL") ||
            raise ArgumentError,
                  "FAVN_RUNNER_POOL is required when FAVN_CONTROL_PLANE_NODE is configured"

        lifecycle_mode =
          case Map.get(environment, "FAVN_RUNNER_LIFECYCLE_MODE", "elastic") do
            "elastic" -> :elastic
            "resident" -> :resident
            value -> raise ArgumentError, "invalid FAVN_RUNNER_LIFECYCLE_MODE: #{inspect(value)}"
          end

        max_uptime_ms =
          positive_integer(
            Map.get(environment, "FAVN_RUNNER_MAX_UPTIME_MS", "3600000"),
            "FAVN_RUNNER_MAX_UPTIME_MS"
          )

        [
          {FavnRunner.TaskResultBuffer, []},
          {DynamicSupervisor, strategy: :one_for_one, name: FavnRunner.TaskExecutorSupervisor},
          {FavnRunner.ControlPlaneConnection, node: control_plane_node},
          Supervisor.child_spec(
            {FavnRunner.RunnerAgent,
             runner_pool: runner_pool,
             runner_instance_id: Map.get(environment, "FAVN_RUNNER_INSTANCE_ID"),
             lifecycle_mode: lifecycle_mode,
             max_uptime_ms: max_uptime_ms},
            restart: :transient
          )
        ]
    end
  end

  defp positive_integer(value, name) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> integer
      _other -> raise ArgumentError, "invalid #{name}: #{inspect(value)}"
    end
  end
end
