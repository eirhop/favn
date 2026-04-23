defmodule Favn.Dev.RuntimeLaunch do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.Paths

  @spec runner_spec(map(), keyword(), map(), map()) :: map()
  def runner_spec(runtime, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    code = "{:ok, _} = Application.ensure_all_started(:favn_runner); Process.sleep(:infinity)"

    base_args = [
      "--sname",
      node_names.runner_short,
      "--cookie",
      secrets["rpc_cookie"]
    ]

    code_path_args =
      opts
      |> ConsumerCodePath.ebin_paths()
      |> Enum.flat_map(fn path -> ["-pa", path] end)

    args =
      base_args ++
        code_path_args ++ ["-S", "mix", "run", "--no-compile", "--no-start", "--eval", code]

    %{
      name: "runner",
      exec: elixir,
      args: args,
      cwd: runtime["runner_root"],
      log_path: Paths.runner_log_path(Paths.root_dir(opts)),
      env: runtime_env()
    }
  end

  @spec orchestrator_spec(map(), Config.t(), keyword(), map(), map()) :: map()
  def orchestrator_spec(runtime, %Config{} = config, opts, node_names, secrets)
      when is_map(runtime) and is_list(opts) and is_map(node_names) and is_map(secrets) do
    elixir = System.find_executable("elixir") || "elixir"
    sqlite_path = Path.expand(config.sqlite_path, Paths.root_dir(opts))

    code =
      """
      storage = System.get_env("FAVN_DEV_STORAGE", "memory")

      cond do
        storage == "sqlite" ->
          {:ok, _} = Application.ensure_all_started(:favn_storage_sqlite)
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.SQLite)
          Application.put_env(:favn_orchestrator, :storage_adapter_opts, database: System.get_env("FAVN_DEV_SQLITE_PATH"))

        storage == "postgres" ->
          {:ok, _} = Application.ensure_all_started(:favn_storage_postgres)
          Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.Postgres)

          Application.put_env(
            :favn_orchestrator,
            :storage_adapter_opts,
            hostname: System.get_env("FAVN_DEV_POSTGRES_HOST"),
            port: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_PORT", "5432")),
            username: System.get_env("FAVN_DEV_POSTGRES_USERNAME"),
            password: System.get_env("FAVN_DEV_POSTGRES_PASSWORD"),
            database: System.get_env("FAVN_DEV_POSTGRES_DATABASE"),
            ssl: System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true",
            pool_size: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_POOL_SIZE", "10"))
          )
      end
      runner_node = String.to_atom(System.get_env("FAVN_DEV_RUNNER_NODE"))
      Application.put_env(:favn_orchestrator, :runner_client, FavnOrchestrator.RunnerClient.LocalNode)
      Application.put_env(:favn_orchestrator, :runner_client_opts, [runner_node: runner_node])
      Application.put_env(
        :favn_orchestrator,
        :api_server,
        enabled: System.get_env("FAVN_ORCHESTRATOR_API_ENABLED", "0") == "1",
        port: String.to_integer(System.get_env("FAVN_ORCHESTRATOR_API_PORT", "4101"))
      )

      Application.put_env(
        :favn_orchestrator,
        :api_service_tokens,
        System.get_env("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "")
        |> String.split(",", trim: true)
      )

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
          "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => secrets["service_token"]
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
end
