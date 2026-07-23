defmodule Favn.Dev.Doctor do
  @moduledoc """
  Local project setup validation for Favn development workflows.
  """

  alias Favn.Dev.{ComposeLifecycle, Config, Docker, Install, LocalHttpClient, Paths, State}
  alias Favn.ModuleDiscovery

  @type check :: %{name: String.t(), status: :ok | :error, message: String.t()}

  @spec run(keyword()) :: {:ok, [check()]} | {:error, [check()]}
  def run(opts) when is_list(opts) do
    root_dir = Paths.root_dir(opts) |> Path.expand()

    checks = [
      safe_check("docker", fn -> docker_check(opts) end),
      safe_check("control-plane install", fn -> install_check(opts) end),
      safe_check("compose deployment", fn -> compose_deployment_check(opts) end),
      safe_check("compose runtime", fn -> compose_runtime_check(opts) end),
      safe_check("mix project", fn -> mix_project_check(root_dir) end),
      safe_check("config", fn -> config_file_check(root_dir) end),
      safe_check("asset_modules", fn -> module_config_check(:asset_modules, :assets) end),
      safe_check("pipeline_modules", fn -> module_config_check(:pipeline_modules, :pipelines) end),
      safe_check("connection_modules", fn ->
        module_config_check(:connection_modules, :connections)
      end),
      safe_check("connections", fn -> config_key_check(:connections, :keyword) end),
      safe_check("runner_plugins", fn -> config_key_check(:runner_plugins, :plugin_list) end),
      safe_check("asset_modules loaded", fn -> modules_check(:asset_modules) end),
      safe_check("pipeline_modules loaded", fn -> modules_check(:pipeline_modules) end),
      safe_check("connection_modules loaded", fn -> modules_check(:connection_modules) end),
      safe_check("runner plugins loaded", fn -> runner_plugins_check() end),
      safe_check("connection runtime", fn -> connection_runtime_check() end),
      safe_check("relation catalogs", fn -> relation_catalogs_check() end),
      safe_check("manifest", fn -> manifest_check() end)
    ]

    if Enum.all?(checks, &(&1.status == :ok)) do
      {:ok, checks}
    else
      {:error, checks}
    end
  end

  defp docker_check(opts) do
    case Docker.probe(opts) do
      {:ok, probe} ->
        ok(
          "docker",
          "Docker #{probe.server_version} linux/amd64 with Compose #{probe.compose_version}"
        )

      {:error, reason} ->
        error("docker", docker_error(reason))
    end
  end

  defp install_check(opts) do
    case Install.ensure_ready(opts) do
      :ok -> ok("control-plane install", "immutable control-plane image verified")
      {:error, :install_required} -> error("control-plane install", "run mix favn.install")
      {:error, reason} -> error("control-plane install", "not ready: #{docker_error(reason)}")
    end
  end

  defp compose_deployment_check(opts) do
    with {:ok, path} <- Config.resolve_compose_file(opts),
         :ok <- Docker.probe_compose(opts) do
      ok(
        "compose deployment",
        "consumer-owned Compose file selected: #{Path.relative_to(path, Paths.root_dir(opts))}"
      )
    else
      {:error, {:compose_file_missing, _path}} ->
        error("compose deployment", "run mix favn.init")

      {:error, reason} ->
        error("compose deployment", docker_error(reason))
    end
  end

  defp compose_runtime_check(opts) do
    case ComposeLifecycle.status(opts) do
      %{stack_status: :stopped} ->
        ok("compose runtime", "project stack is installed and currently stopped")

      %{stack_status: :running, services: services, user_urls: %{orchestrator_api: url}} = status ->
        with true <- services_ready?(services),
             :ok <- control_plane_ready(url),
             :ok <-
               runtime_alignment_ready(status.runner, status.active_manifest_version_id, opts) do
          ok(
            "compose runtime",
            "PostgreSQL, runner, manifest alignment, and control plane are ready"
          )
        else
          _not_ready ->
            error("compose runtime", "running services are not fully ready or aligned")
        end

      %{stack_status: state} ->
        error("compose runtime", "project stack state is #{state}")
    end
  end

  defp services_ready?(services) do
    Enum.all?([:postgres, :runner, :control_plane], fn role ->
      case Map.get(services, role) do
        %{status: :running, health: health} when health in [:healthy, :none] -> true
        _unavailable -> false
      end
    end)
  end

  defp control_plane_ready(url) when is_binary(url) do
    case LocalHttpClient.request(:get, url <> "/api/orchestrator/v1/health/ready") do
      {:ok, %{"data" => %{"status" => "ready"}}} -> :ok
      {:ok, %{"status" => "ready"}} -> :ok
      _not_ready -> {:error, :control_plane_not_ready}
    end
  end

  defp runtime_alignment_ready(
         %{"runner_release_id" => runner_release_id},
         manifest_version_id,
         opts
       )
       when is_binary(runner_release_id) and is_binary(manifest_version_id) do
    case State.read_runtime(opts) do
      {:ok,
       %{
         "runner_release_id" => ^runner_release_id,
         "active_manifest_version_id" => ^manifest_version_id
       }} ->
        :ok

      _stale ->
        {:error, :runtime_state_mismatch}
    end
  end

  defp runtime_alignment_ready(_runner, _manifest_version_id, _opts),
    do: {:error, :runtime_state_unavailable}

  defp docker_error(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp docker_error({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)
  defp docker_error({reason, _status, _output}) when is_atom(reason), do: Atom.to_string(reason)
  defp docker_error(_reason), do: "validation failed"

  defp mix_project_check(root_dir) do
    path_check("mix project", Path.join(root_dir, "mix.exs"), "mix.exs found")
  end

  defp config_file_check(root_dir) do
    path_check("config", Path.join([root_dir, "config", "config.exs"]), "config/config.exs found")
  end

  defp path_check(name, path, ok_message) do
    if File.exists?(path) do
      ok(name, ok_message)
    else
      error(name, "missing #{Path.basename(path)}")
    end
  end

  defp config_key_check(key, shape) do
    case Application.get_env(:favn, key) do
      value when is_list(value) and value != [] ->
        validate_config_shape(key, value, shape)

      _other ->
        error(to_string(key), "missing or empty config :favn, #{key}")
    end
  end

  defp validate_config_shape(key, value, :module_list) do
    if Enum.all?(value, &is_atom/1) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected a non-empty list of modules")
    end
  end

  defp validate_config_shape(key, value, :keyword) do
    if Keyword.keyword?(value) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected a non-empty keyword list")
    end
  end

  defp validate_config_shape(key, value, :plugin_list) do
    if Enum.all?(value, &valid_plugin_entry?/1) do
      ok(to_string(key), "configured")
    else
      error(to_string(key), "expected plugin modules or {module, keyword_opts} entries")
    end
  end

  defp module_config_check(key, discovery_key) do
    discovery = Application.get_env(:favn, :discovery, [])

    case Application.get_env(:favn, key, :unset) do
      modules when is_list(modules) and modules != [] ->
        validate_config_shape(key, modules, :module_list)

      :all ->
        discovery_config_check(key, discovery)

      :unset ->
        if discovery_enabled?(discovery, discovery_key) do
          discovery_config_check(key, discovery)
        else
          error(to_string(key), "missing or empty config :favn, #{key}")
        end

      _other ->
        error(to_string(key), "expected a non-empty list of modules or :all")
    end
  end

  defp discovery_config_check(key, discovery) when is_list(discovery) do
    case Keyword.get(discovery, :apps, []) do
      apps when is_list(apps) and apps != [] ->
        if Enum.all?(apps, &is_atom/1) do
          ok(to_string(key), "configured by discovery")
        else
          error(to_string(key), "discovery apps must be atoms")
        end

      _other ->
        error(to_string(key), "discovery requires non-empty :apps")
    end
  end

  defp discovery_config_check(key, _discovery) do
    error(to_string(key), "discovery config must be a keyword list")
  end

  defp modules_check(key) do
    case fetch_module_list(key) do
      {:ok, modules} ->
        missing = Enum.reject(modules, &Code.ensure_loaded?/1)

        case missing do
          [] -> ok(to_string(key) <> " loaded", "#{length(modules)} module(s) available")
          modules -> error(to_string(key) <> " loaded", "not loaded: #{inspect(modules)}")
        end

      {:error, message} ->
        error(to_string(key) <> " loaded", message)
    end
  end

  defp runner_plugins_check do
    case fetch_plugin_list() do
      {:ok, plugins} ->
        missing =
          plugins
          |> Enum.map(&plugin_module/1)
          |> Enum.reject(&Code.ensure_loaded?/1)

        case missing do
          [] -> ok("runner plugins loaded", "#{length(plugins)} plugin(s) available")
          modules -> error("runner plugins loaded", "not loaded: #{inspect(modules)}")
        end

      {:error, message} ->
        error("runner plugins loaded", message)
    end
  end

  defp connection_runtime_check do
    with {:ok, modules} <- fetch_module_list(:connection_modules),
         {:ok, runtime} <- fetch_keyword(:connections) do
      runtime_names = runtime |> Keyword.keys() |> MapSet.new()

      missing =
        modules
        |> Enum.flat_map(fn module ->
          if Code.ensure_loaded?(module) and function_exported?(module, :definition, 0) do
            definition = module.definition()
            if MapSet.member?(runtime_names, definition.name), do: [], else: [definition.name]
          else
            []
          end
        end)

      case missing do
        [] -> ok("connection runtime", "runtime config exists for connection definitions")
        names -> error("connection runtime", "missing runtime config for #{inspect(names)}")
      end
    else
      {:error, message} -> error("connection runtime", message)
    end
  end

  defp manifest_check do
    case FavnAuthoring.list_assets() do
      {:ok, assets} ->
        ok("manifest", "compiled #{length(assets)} asset(s) for manifest generation")

      {:error, reason} ->
        error("manifest", "asset compilation failed: #{inspect(reason)}")
    end
  end

  defp relation_catalogs_check do
    with {:assets, {:ok, assets}} <- {:assets, FavnAuthoring.list_assets()},
         requirements <- relation_catalog_requirements(assets),
         {:ok, connections} <- resolve_catalog_connections(requirements),
         :ok <- validate_relation_catalogs(requirements, connections) do
      ok("relation catalogs", relation_catalogs_ok_message(requirements, connections))
    else
      {:assets, {:error, reason}} ->
        error("relation catalogs", "asset compilation failed: #{inspect(redact(reason))}")

      {:error, {:connections, errors}} ->
        error(
          "relation catalogs",
          "connection resolution failed: #{format_connection_errors(errors)}"
        )

      {:error, messages} when is_list(messages) ->
        error("relation catalogs", Enum.join(messages, "; "))
    end
  end

  defp relation_catalog_requirements(assets) do
    assets
    |> Enum.flat_map(&asset_relation_requirement/1)
    |> Enum.group_by(& &1.connection, & &1.catalog)
    |> Map.new(fn {connection, catalogs} ->
      {connection, catalogs |> Enum.uniq() |> Enum.sort()}
    end)
  end

  defp asset_relation_requirement(asset) do
    relation = Map.get(asset, :relation)
    connection = relation_field(relation, :connection)
    catalog = relation_field(relation, :catalog)

    if is_atom(connection) and not is_nil(connection) and
         (is_binary(catalog) or is_nil(catalog)) do
      [%{connection: connection, catalog: catalog}]
    else
      []
    end
  end

  defp relation_field(nil, _field), do: nil
  defp relation_field(relation, field) when is_map(relation), do: Map.get(relation, field)
  defp relation_field(_relation, _field), do: nil

  defp resolve_catalog_connections(requirements) when map_size(requirements) == 0, do: {:ok, %{}}

  defp resolve_catalog_connections(requirements) do
    requirements
    |> Map.keys()
    |> Favn.Connection.Loader.resolve_required()
    |> case do
      {:ok, connections} -> {:ok, connections}
      {:error, errors} -> {:error, {:connections, errors}}
    end
  end

  defp validate_relation_catalogs(requirements, connections) do
    messages =
      Enum.flat_map(requirements, fn {connection_name, catalogs} ->
        resolved = Map.fetch!(connections, connection_name)
        adapter = resolved.adapter

        if function_exported?(adapter, :configured_catalogs, 1) do
          validate_adapter_catalogs(resolved, catalogs)
        else
          []
        end
      end)

    if messages == [], do: :ok, else: {:error, messages}
  end

  defp validate_adapter_catalogs(resolved, required_catalogs) do
    case resolved.adapter.configured_catalogs(resolved) do
      {:ok, configured_catalogs} ->
        configured = normalize_catalog_set(configured_catalogs)
        {catalogless?, qualified_catalogs} = split_required_catalogs(required_catalogs)
        missing = Enum.reject(qualified_catalogs, &MapSet.member?(configured, &1))

        missing_catalog_messages(resolved, missing) ++
          catalogless_relation_messages(resolved, configured, catalogless?)

      {:error, reason} ->
        [
          "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} could not report configured catalogs: #{inspect(redact_reason(reason))}"
        ]

      other ->
        [
          "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} returned invalid configured_catalogs/1 result #{inspect(redact_reason(other))}"
        ]
    end
  rescue
    error ->
      [
        "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} raised during configured_catalogs/1: #{inspect(error.__struct__)}"
      ]
  catch
    kind, reason ->
      [
        "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} failed configured_catalogs/1: #{kind} #{inspect(redact_reason(reason))}"
      ]
  end

  defp split_required_catalogs(required_catalogs) do
    {catalogless, qualified} = Enum.split_with(required_catalogs, &is_nil/1)
    {catalogless != [], qualified}
  end

  defp missing_catalog_messages(_resolved, []), do: []

  defp missing_catalog_messages(resolved, missing) do
    [
      "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} is missing configured catalog(s) #{inspect(missing)}"
    ]
  end

  defp catalogless_relation_messages(_resolved, _configured, false), do: []

  defp catalogless_relation_messages(resolved, configured, true) do
    case adapter_default_catalog(resolved) do
      {:ok, catalog} when is_binary(catalog) ->
        if MapSet.member?(configured, catalog) do
          []
        else
          [
            "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} default catalog #{inspect(catalog)} is not attached"
          ]
        end

      {:ok, nil} ->
        [
          "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} has catalogless asset relation(s); configure relation.catalog or a default attached catalog"
        ]

      {:error, reason} ->
        [
          "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} could not report default catalog: #{inspect(redact_reason(reason))}"
        ]

      other ->
        [
          "connection #{inspect(resolved.name)} adapter #{inspect(resolved.adapter)} returned invalid default_catalog/1 result #{inspect(redact_reason(other))}"
        ]
    end
  end

  defp adapter_default_catalog(resolved) do
    if function_exported?(resolved.adapter, :default_catalog, 1) do
      case resolved.adapter.default_catalog(resolved) do
        {:ok, catalog} when is_binary(catalog) -> {:ok, catalog}
        {:ok, nil} -> {:ok, nil}
        {:ok, catalog} when is_atom(catalog) -> {:ok, Atom.to_string(catalog)}
        other -> other
      end
    else
      {:ok, nil}
    end
  end

  defp normalize_catalog_set(%MapSet{} = catalogs) do
    catalogs
    |> Enum.flat_map(&normalize_catalog/1)
    |> MapSet.new()
  end

  defp normalize_catalog_set(catalogs) when is_list(catalogs) do
    catalogs
    |> Enum.flat_map(&normalize_catalog/1)
    |> MapSet.new()
  end

  defp normalize_catalog_set(catalogs) when is_map(catalogs) do
    catalogs
    |> Map.keys()
    |> Enum.flat_map(&normalize_catalog/1)
    |> MapSet.new()
  end

  defp normalize_catalog_set(catalog), do: MapSet.new(normalize_catalog(catalog))

  defp normalize_catalog(catalog) when is_binary(catalog), do: [catalog]
  defp normalize_catalog(catalog) when is_atom(catalog), do: [Atom.to_string(catalog)]
  defp normalize_catalog(_catalog), do: []

  defp relation_catalogs_ok_message(requirements, _connections)
       when map_size(requirements) == 0 do
    "no catalog-qualified asset relations found"
  end

  defp relation_catalogs_ok_message(requirements, connections) do
    checked =
      requirements
      |> Map.keys()
      |> Enum.count(fn name ->
        resolved = Map.fetch!(connections, name)
        function_exported?(resolved.adapter, :configured_catalogs, 1)
      end)

    skipped = map_size(requirements) - checked

    "validated #{checked} connection(s), skipped #{skipped} adapter(s) without configured_catalogs/1"
  end

  defp format_connection_errors(errors) when is_list(errors) do
    errors
    |> Enum.map(fn error ->
      message = Map.get(error, :message, inspect(redact(error)))
      connection = Map.get(error, :connection)

      if connection do
        "#{inspect(connection)}: #{message}"
      else
        message
      end
    end)
    |> Enum.join("; ")
  end

  defp format_connection_errors(error), do: format_connection_errors([error])

  defp redact_reason(reason) when is_atom(reason), do: reason
  defp redact_reason(reason) when is_binary(reason), do: :redacted
  defp redact_reason(reason), do: redact(reason)

  defp redact(value) when is_map(value) do
    Map.new(value, fn {key, child} -> {key, redact_value(key, child)} end)
  end

  defp redact(value) when is_list(value), do: Enum.map(value, &redact/1)

  defp redact(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&redact/1) |> List.to_tuple()
  end

  defp redact(value), do: value

  defp redact_value(key, _value) when key in [:password, :secret, :token, :key, :authorization],
    do: "[REDACTED]"

  defp redact_value(key, value) when is_atom(key) do
    if key |> Atom.to_string() |> sensitive_key?(), do: "[REDACTED]", else: redact(value)
  end

  defp redact_value(key, value) when is_binary(key) do
    if sensitive_key?(key), do: "[REDACTED]", else: redact(value)
  end

  defp redact_value(_key, value), do: redact(value)

  defp sensitive_key?(key) when is_binary(key) do
    key = String.downcase(key)

    String.contains?(key, "secret") or String.contains?(key, "password") or
      String.contains?(key, "token")
  end

  defp plugin_module({module, _opts}) when is_atom(module), do: module
  defp plugin_module(module) when is_atom(module), do: module
  defp plugin_module(other), do: other

  defp valid_plugin_entry?(module) when is_atom(module), do: true

  defp valid_plugin_entry?({module, opts}) when is_atom(module) and is_list(opts),
    do: Keyword.keyword?(opts)

  defp valid_plugin_entry?(_other), do: false

  defp fetch_module_list(key) do
    discovery = Application.get_env(:favn, :discovery, [])
    discovery_key = module_discovery_key(key)

    case Application.get_env(:favn, key, :unset) do
      :unset when discovery_key != nil ->
        if discovery_enabled?(discovery, discovery_key) do
          discover_module_list(discovery_key, discovery)
        else
          {:ok, []}
        end

      :all when discovery_key != nil ->
        discover_module_list(discovery_key, discovery)

      modules when is_list(modules) ->
        if Enum.all?(modules, &is_atom/1), do: {:ok, modules}, else: {:error, "expected modules"}

      _other ->
        {:error, "expected a list"}
    end
  end

  defp discover_module_list(discovery_key, discovery) do
    case ModuleDiscovery.discover(discovery_key, discovery) do
      {:ok, modules} -> {:ok, modules}
      {:error, reason} -> {:error, "discovery failed: #{inspect(reason)}"}
    end
  end

  defp module_discovery_key(:asset_modules), do: :assets
  defp module_discovery_key(:pipeline_modules), do: :pipelines
  defp module_discovery_key(:connection_modules), do: :connections
  defp module_discovery_key(_key), do: nil

  defp discovery_enabled?(discovery, key) when is_list(discovery),
    do: Keyword.get(discovery, key) == :all

  defp discovery_enabled?(_discovery, _key), do: false

  defp fetch_keyword(key) do
    case Application.get_env(:favn, key, []) do
      values when is_list(values) ->
        if Keyword.keyword?(values), do: {:ok, values}, else: {:error, "expected a keyword list"}

      _other ->
        {:error, "expected a keyword list"}
    end
  end

  defp fetch_plugin_list do
    case Application.get_env(:favn, :runner_plugins, []) do
      plugins when is_list(plugins) ->
        if Enum.all?(plugins, &valid_plugin_entry?/1),
          do: {:ok, plugins},
          else: {:error, "expected plugin entries"}

      _other ->
        {:error, "expected a plugin list"}
    end
  end

  defp safe_check(name, fun) when is_function(fun, 0) do
    fun.()
  rescue
    error -> error(name, Exception.message(error))
  catch
    kind, reason -> error(name, "#{kind}: #{inspect(reason)}")
  end

  defp ok(name, message), do: %{name: name, status: :ok, message: message}
  defp error(name, message), do: %{name: name, status: :error, message: message}
end
