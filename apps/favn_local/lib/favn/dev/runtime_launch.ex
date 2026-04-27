defmodule Favn.Dev.RuntimeLaunch do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.Paths

  @spec runner_spec(map(), keyword(), map(), map()) :: map()
  def runner_spec(runtime, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"

    code =
      """
      delimiter = if match?({:win32, _}, :os.type()), do: ";", else: ":"

      System.get_env("FAVN_DEV_CONSUMER_EBIN_PATHS", "")
      |> String.split(delimiter, trim: true)
      |> Enum.each(&Code.prepend_path/1)

      System.get_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", "")
      |> case do
        "" -> []
        encoded -> encoded |> Base.decode64!() |> :erlang.binary_to_term()
      end
      |> Enum.each(fn {key, value} -> Application.put_env(:favn, key, value) end)

      {:ok, _} = Application.ensure_all_started(:favn_runner)
      Process.sleep(:infinity)
      """
      |> String.trim()

    base_args = [
      "--sname",
      node_names.runner_short,
      "--cookie",
      secrets["rpc_cookie"]
    ]

    consumer_ebin_paths = ConsumerCodePath.ebin_paths(opts)

    args =
      base_args ++
        ["-S", "mix", "run", "--no-compile", "--no-start", "--eval", code]

    %{
      name: "runner",
      exec: elixir,
      args: args,
      cwd: runtime["runner_root"],
      log_path: Paths.runner_log_path(Paths.root_dir(opts)),
      env:
        Map.put(
          runtime_env(),
          "FAVN_DEV_CONSUMER_EBIN_PATHS",
          Enum.join(consumer_ebin_paths, path_separator())
        )
        |> Map.put("FAVN_DEV_CONSUMER_FAVN_CONFIG", encoded_consumer_favn_config(opts))
    }
  end

  @spec orchestrator_spec(map(), Config.t(), keyword(), map(), map()) :: map()
  def orchestrator_spec(runtime, %Config{} = config, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    sqlite_path = Path.expand(config.sqlite_path, Paths.root_dir(opts))

    code =
      """
      storage = System.fetch_env!("FAVN_DEV_STORAGE")

      Application.put_env(
        :favn_orchestrator,
        :api_server,
        enabled: System.get_env("FAVN_ORCHESTRATOR_API_ENABLED", "0") == "1",
        port: String.to_integer(System.fetch_env!("FAVN_ORCHESTRATOR_API_PORT"))
      )

      Application.put_env(
        :favn_orchestrator,
        :api_service_tokens,
        System.fetch_env!("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS")
        |> String.split(",", trim: true)
      )

      case storage do
        "memory" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, FavnOrchestrator.Storage.Adapter.Memory)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

        "sqlite" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.SQLite)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts,
            database: System.fetch_env!("FAVN_DEV_SQLITE_PATH"),
            migration_mode: :auto
          )
          {:ok, _} = Application.ensure_all_started(:favn_storage_sqlite)

        "postgres" ->
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.Postgres)

          Application.put_env(
            :favn_orchestrator,
            :storage_adapter_opts,
            hostname: System.fetch_env!("FAVN_DEV_POSTGRES_HOST"),
            port: String.to_integer(System.fetch_env!("FAVN_DEV_POSTGRES_PORT")),
            username: System.fetch_env!("FAVN_DEV_POSTGRES_USERNAME"),
            password: System.fetch_env!("FAVN_DEV_POSTGRES_PASSWORD"),
            database: System.fetch_env!("FAVN_DEV_POSTGRES_DATABASE"),
            ssl: System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true",
            pool_size: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_POOL_SIZE", "10"))
          )

          {:ok, _} = Application.ensure_all_started(:favn_storage_postgres)

        other ->
          raise ArgumentError,
                "unsupported FAVN_DEV_STORAGE=\#{inspect(other)}; expected memory, sqlite, or postgres"
      end

      runner_node = String.to_atom(System.fetch_env!("FAVN_DEV_RUNNER_NODE"))
      Application.put_env(:favn_orchestrator, :runner_client, FavnOrchestrator.RunnerClient.LocalNode)
      Application.put_env(:favn_orchestrator, :runner_client_opts, [runner_node: runner_node])
      Application.put_env(:favn_orchestrator, :scheduler, enabled: true, tick_ms: 15_000)

      {:ok, _} = Application.ensure_all_started(:favn_orchestrator)
      Process.sleep(:infinity)
      """
      |> String.trim()

    %{
      name: "orchestrator",
      exec: elixir,
      args: [
        "--sname",
        node_names.orchestrator_short,
        "--cookie",
        secrets["rpc_cookie"],
        "-S",
        "mix",
        "run",
        "--no-compile",
        "--no-start",
        "--eval",
        code
      ],
      cwd: runtime["orchestrator_root"],
      log_path: Paths.orchestrator_log_path(Paths.root_dir(opts)),
      env:
        Map.merge(runtime_env(), %{
          "FAVN_DEV_STORAGE" => Atom.to_string(config.storage),
          "FAVN_DEV_SQLITE_PATH" => sqlite_path,
          "FAVN_DEV_POSTGRES_HOST" => config.postgres.hostname,
          "FAVN_DEV_POSTGRES_PORT" => Integer.to_string(config.postgres.port),
          "FAVN_DEV_POSTGRES_USERNAME" => config.postgres.username,
          "FAVN_DEV_POSTGRES_PASSWORD" => config.postgres.password,
          "FAVN_DEV_POSTGRES_DATABASE" => config.postgres.database,
          "FAVN_DEV_POSTGRES_SSL" => if(config.postgres.ssl, do: "true", else: "false"),
          "FAVN_DEV_POSTGRES_POOL_SIZE" => Integer.to_string(config.postgres.pool_size),
          "FAVN_DEV_RUNNER_NODE" => node_names.runner_full,
          "FAVN_ORCHESTRATOR_API_ENABLED" =>
            if(config.orchestrator_api_enabled, do: "1", else: "0"),
          "FAVN_ORCHESTRATOR_API_PORT" => Integer.to_string(config.orchestrator_port),
          "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => secrets["service_token"],
          "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME" => secrets["local_operator_username"],
          "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD" => secrets["local_operator_password"],
          "FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME" => "Favn Local Operator",
          "FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES" => "operator"
        })
    }
  end

  @spec web_spec(map(), Config.t(), keyword(), map()) :: map()
  def web_spec(runtime, %Config{} = config, opts, secrets)
      when is_map(runtime) and is_list(opts) and is_map(secrets) do
    node = System.find_executable("node") || "node"
    vite = Path.join(runtime["web_root"], "node_modules/vite/bin/vite.js")

    %{
      name: "web",
      exec: node,
      args: [vite, "preview", "--host", "127.0.0.1", "--port", Integer.to_string(config.web_port)],
      cwd: runtime["web_root"],
      log_path: Paths.web_log_path(Paths.root_dir(opts)),
      env: %{
        "FAVN_ORCHESTRATOR_BASE_URL" => config.orchestrator_base_url,
        "FAVN_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"],
        "FAVN_WEB_SESSION_SECRET" => secrets["web_session_secret"]
      }
    }
  end

  defp runtime_env do
    %{"MIX_ENV" => "dev"}
  end

  defp path_separator do
    case :os.type() do
      {:win32, _name} -> ";"
      _other -> ":"
    end
  end

  defp encoded_consumer_favn_config(opts) do
    root_dir = Paths.root_dir(opts)

    [:connection_modules, :connections, :runner_plugins, :duckdb_in_process_client]
    |> Enum.flat_map(fn key ->
      case Application.fetch_env(:favn, key) do
        {:ok, value} -> [{key, normalize_consumer_favn_config(key, value, root_dir)}]
        :error -> []
      end
    end)
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  defp normalize_consumer_favn_config(:connections, connections, root_dir) do
    cond do
      Keyword.keyword?(connections) ->
        Keyword.new(connections, fn {name, config} -> {name, absolutize_connection_paths(config, root_dir)} end)

      is_map(connections) ->
        Map.new(connections, fn {name, config} -> {name, absolutize_connection_paths(config, root_dir)} end)

      true ->
        connections
    end
  end

  defp normalize_consumer_favn_config(_key, value, _root_dir), do: value

  defp absolutize_connection_paths(config, root_dir) when is_list(config) do
    if Keyword.has_key?(config, :database) do
      Keyword.update!(config, :database, &expand_relative_path(&1, root_dir))
    else
      config
    end
  end

  defp absolutize_connection_paths(config, root_dir) when is_map(config) do
    if Map.has_key?(config, :database) do
      Map.update!(config, :database, &expand_relative_path(&1, root_dir))
    else
      config
    end
  end

  defp absolutize_connection_paths(config, _root_dir), do: config

  defp expand_relative_path(path, root_dir) when is_binary(path) do
    case Path.type(path) do
      :relative -> Path.expand(path, root_dir)
      _other -> path
    end
  end

  defp expand_relative_path(path, _root_dir), do: path
end
