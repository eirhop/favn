defmodule Favn.Dev.RuntimeLaunchTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Config
  alias Favn.Dev.ConsumerCodePath
  alias Favn.Dev.ConsumerConfigTransport
  alias Favn.Dev.RuntimeLaunch

  test "runner, orchestrator, and web specs target installed runtime roots" do
    runtime = %{
      "materialized_root" => "/tmp/favn_runtime",
      "runner_root" => "/tmp/favn_runtime",
      "orchestrator_root" => "/tmp/favn_runtime",
      "web_root" => "/tmp/favn_runtime/web/favn_web"
    }

    config = Config.resolve(storage: :sqlite)

    node_names = %{
      runner_short: "favn_runner_test",
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test",
      orchestrator_full: "favn_orchestrator_test@host"
    }

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "web_session_secret" => "secret"
    }

    opts = distribution_opts()
    runner = RuntimeLaunch.runner_spec(runtime, opts, node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, opts, node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, opts, secrets)

    assert runner.cwd == runtime["runner_root"]
    assert orchestrator.cwd == runtime["orchestrator_root"]
    assert web.cwd == runtime["web_root"]
    assert "--no-compile" in runner.args
    assert "mix" in runner.args
    assert "--no-compile" in orchestrator.args
    assert "mix" in orchestrator.args
    assert web.exec == (System.find_executable("node") || "node")
    assert hd(web.args) == Path.join(runtime["web_root"], "node_modules/vite/bin/vite.js")
    assert "preview" in web.args
    assert web.env["FAVN_WEB_PUBLIC_ORIGIN"] == config.web_base_url
    assert web.env["FAVN_WEB_LOCAL_DEV_TRUSTED_AUTH"] == "1"
  end

  test "runtime specs bind local HTTP and distributed Erlang to loopback" do
    root_dir = "/tmp/favn_loopback_launch"

    runtime = %{
      "runner_root" => "/tmp/favn_runtime",
      "orchestrator_root" => "/tmp/favn_runtime",
      "web_root" => "/tmp/favn_runtime/web/favn_web"
    }

    config = Config.resolve(orchestrator_port: 4101, web_port: 4173)

    node_names = %{
      runner_short: "favn_runner_test",
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "web_session_secret" => "secret"
    }

    opts = distribution_opts(root_dir: root_dir)
    runner = RuntimeLaunch.runner_spec(runtime, opts, node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, opts, node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, opts, secrets)
    runner_port = RuntimeLaunch.distribution_port(:runner, opts)
    orchestrator_port = RuntimeLaunch.distribution_port(:orchestrator, opts)
    runner_erl = erl_flag!(runner)
    orchestrator_erl = erl_flag!(orchestrator)
    code = eval_code!(orchestrator)

    assert runner_erl =~ "inet_dist_use_interface {127,0,1,1}"
    assert runner_erl =~ "inet_dist_listen_min #{runner_port}"
    assert runner_erl =~ "inet_dist_listen_max #{runner_port}"
    assert orchestrator_erl =~ "inet_dist_use_interface {127,0,1,1}"
    assert orchestrator_erl =~ "inet_dist_listen_min #{orchestrator_port}"
    assert orchestrator_erl =~ "inet_dist_listen_max #{orchestrator_port}"
    assert runner.env["ERL_EPMD_ADDRESS"] == "127.0.1.1"
    assert orchestrator.env["ERL_EPMD_ADDRESS"] == "127.0.1.1"
    assert orchestrator.env["FAVN_ORCHESTRATOR_API_BIND_IP"] == "127.0.0.1"
    assert code =~ "bind_ip: api_bind_ip"
    assert web.args == [hd(web.args), "preview", "--host", "127.0.0.1", "--port", "4173"]
  end

  test "orchestrator spec handles memory storage explicitly" do
    runtime = %{
      "orchestrator_root" => "/tmp/favn_runtime"
    }

    config = Config.resolve(storage: :memory)

    node_names = %{
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token"
    }

    orchestrator =
      RuntimeLaunch.orchestrator_spec(runtime, config, distribution_opts(), node_names, secrets)

    code = eval_code!(orchestrator)

    assert orchestrator.env["FAVN_DEV_STORAGE"] == "memory"
    refute Map.has_key?(orchestrator.env, "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME")
    refute Map.has_key?(orchestrator.env, "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD")
    assert code =~ ~s("memory" ->)
    assert code =~ "FavnOrchestrator.Storage.Adapter.Memory"
    assert code =~ "unsupported FAVN_DEV_STORAGE"
  end

  test "orchestrator spec configures storage before starting orchestrator" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve(storage: :sqlite)

    node_names = %{
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    code =
      runtime
      |> RuntimeLaunch.orchestrator_spec(config, distribution_opts(), node_names, secrets)
      |> eval_code!()

    assert before?(
             code,
             "Application.put_env(:favn_orchestrator, :storage_adapter",
             "Application.ensure_all_started(:favn_storage_sqlite)"
           )

    assert before?(
             code,
             "Application.ensure_all_started(:favn_storage_sqlite)",
             "Application.ensure_all_started(:favn_orchestrator)"
           )

    assert code =~ "migration_mode: :auto"
  end

  test "consumer code path excludes runtime-owned favn apps" do
    build_path =
      Path.join(
        System.tmp_dir!(),
        "favn_consumer_code_path_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      File.rm_rf(build_path)
    end)

    assert :ok = File.mkdir_p(Path.join(build_path, "lib/favn_runner/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/favn_local/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/my_app/ebin"))
    assert :ok = File.mkdir_p(Path.join(build_path, "lib/jason/ebin"))

    assert ConsumerCodePath.ebin_paths(build_path: build_path) == [
             Path.join(build_path, "lib/jason/ebin"),
             Path.join(build_path, "lib/my_app/ebin")
           ]
  end

  test "runner spec prepends consumer code paths after mix initializes" do
    runtime = %{
      "runner_root" => "/tmp/favn_runtime"
    }

    node_names = %{
      runner_short: "favn_runner_test"
    }

    secrets = %{
      "rpc_cookie" => "cookie"
    }

    build_path =
      Path.join(
        System.tmp_dir!(),
        "favn_runner_consumer_path_#{System.unique_integer([:positive])}"
      )

    on_exit(fn ->
      File.rm_rf(build_path)
    end)

    runtime_owned_path = Path.join(build_path, "lib/favn_runner/ebin")
    consumer_path = Path.join(build_path, "lib/my_app/ebin")

    assert :ok = File.mkdir_p(runtime_owned_path)
    assert :ok = File.mkdir_p(consumer_path)

    runner =
      RuntimeLaunch.runner_spec(
        runtime,
        distribution_opts(build_path: build_path),
        node_names,
        secrets
      )

    code = eval_code!(runner)

    refute "-pa" in runner.args
    assert runner.env["FAVN_DEV_CONSUMER_EBIN_PATHS"] == consumer_path
    refute runner.env["FAVN_DEV_CONSUMER_EBIN_PATHS"] == runtime_owned_path
    assert code =~ "FAVN_DEV_CONSUMER_EBIN_PATHS"
    assert code =~ "Code.prepend_path"
    assert before?(code, "Code.prepend_path", "Application.ensure_all_started(:favn_runner)")
  end

  test "runner spec carries consumer favn runtime config before runner startup" do
    runtime = %{
      "runner_root" => "/tmp/favn_runtime"
    }

    node_names = %{
      runner_short: "favn_runner_test"
    }

    secrets = %{
      "rpc_cookie" => "cookie"
    }

    previous_connection_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)
    previous_runner_plugins = Application.get_env(:favn, :runner_plugins)

    Application.put_env(:favn, :connection_modules, [MyApp.Connections.Warehouse])
    Application.put_env(:favn, :connections, warehouse: [database: "warehouse.duckdb"])
    Application.put_env(:favn, :runner_plugins, [{FavnDuckdb, execution_mode: :in_process}])

    on_exit(fn ->
      restore_env(:connection_modules, previous_connection_modules)
      restore_env(:connections, previous_connections)
      restore_env(:runner_plugins, previous_runner_plugins)
    end)

    runner =
      RuntimeLaunch.runner_spec(
        runtime,
        distribution_opts(root_dir: "/tmp/consumer"),
        node_names,
        secrets
      )

    code = eval_code!(runner)

    decoded_config =
      ConsumerConfigTransport.decode(runner.env["FAVN_DEV_CONSUMER_FAVN_CONFIG"])
      |> then(fn {:ok, config} -> config end)

    assert {:connection_modules, [MyApp.Connections.Warehouse]} in decoded_config

    assert {:connections, [warehouse: [database: "/tmp/consumer/warehouse.duckdb"]]} in decoded_config

    assert {:runner_plugins, [{FavnDuckdb, [execution_mode: :in_process]}]} in decoded_config
    assert code =~ "FAVN_DEV_CONSUMER_FAVN_CONFIG"
    assert code =~ "Base.decode64(encoded)"
    assert code =~ ":erlang.binary_to_term(binary, [:safe])"

    assert before?(
             code,
             "Application.put_env(:favn, key, value)",
             "Application.ensure_all_started(:favn_runner)"
           )
  end

  test "orchestrator eval validates runner node env before atom conversion" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve([])

    node_names = %{
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    code =
      runtime
      |> RuntimeLaunch.orchestrator_spec(config, distribution_opts(), node_names, secrets)
      |> eval_code!()

    assert code =~ "validate_runner_node_name!"
    assert before?(code, "validate_runner_node_name!", "String.to_atom()")
  end

  test "orchestrator spec disables scheduler by default" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve([])

    node_names = %{
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    orchestrator =
      RuntimeLaunch.orchestrator_spec(runtime, config, distribution_opts(), node_names, secrets)

    code = eval_code!(orchestrator)

    assert orchestrator.env["FAVN_DEV_SCHEDULER_ENABLED"] == "0"
    assert code =~ "FAVN_DEV_SCHEDULER_ENABLED"
    refute code =~ "enabled: true"
  end

  test "runtime specs propagate loaded env under Favn-owned explicit values" do
    runtime = %{
      "runner_root" => "/tmp/favn_runtime",
      "orchestrator_root" => "/tmp/favn_runtime",
      "web_root" => "/tmp/favn_runtime/web/favn_web"
    }

    config = Config.resolve(storage: :sqlite)

    node_names = %{
      runner_short: "favn_runner_test",
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "web_session_secret" => "secret"
    }

    opts =
      distribution_opts(
        env_file_loaded: %{
          "CUSTOM_ENV" => "from-file",
          "MIX_ENV" => "prod",
          "FAVN_DEV_STORAGE" => "postgres",
          "FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN" => "from-file"
        }
      )

    runner = RuntimeLaunch.runner_spec(runtime, opts, node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, opts, node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, opts, secrets)

    assert runner.env["CUSTOM_ENV"] == "from-file"
    assert orchestrator.env["CUSTOM_ENV"] == "from-file"
    assert web.env["CUSTOM_ENV"] == "from-file"

    assert runner.env["MIX_ENV"] == "dev"
    assert orchestrator.env["MIX_ENV"] == "dev"
    assert orchestrator.env["FAVN_DEV_STORAGE"] == "sqlite"
    assert web.env["FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN"] == "token"
  end

  test "orchestrator spec enables scheduler when resolved config enables it" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve(scheduler: true)

    node_names = %{
      runner_full: "favn_runner_test@host",
      orchestrator_short: "favn_orchestrator_test"
    }

    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    orchestrator =
      RuntimeLaunch.orchestrator_spec(runtime, config, distribution_opts(), node_names, secrets)

    assert orchestrator.env["FAVN_DEV_SCHEDULER_ENABLED"] == "1"
  end

  defp eval_code!(%{args: args}) do
    args
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.find_value(fn
      ["--eval", code] -> code
      _other -> nil
    end) || flunk("expected orchestrator args to include --eval code")
  end

  defp distribution_opts(opts \\ []) do
    Keyword.put(opts, :local_distribution,
      localhost: fn -> ~c"testhost.localdomain" end,
      resolver: fn ~c"testhost" -> {:ok, [{127, 0, 1, 1}]} end,
      epmd_executable: "/usr/bin/epmd",
      epmd_names: fn _opts -> :ok end
    )
  end

  defp erl_flag!(%{args: args}) do
    args
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.find_value(fn
      ["--erl", flags] -> flags
      _other -> nil
    end) || flunk("expected args to include --erl flags")
  end

  defp before?(text, earlier, later) do
    earlier_index = :binary.match(text, earlier)
    later_index = :binary.match(text, later)

    match?({_, _}, earlier_index) and match?({_, _}, later_index) and
      elem(earlier_index, 0) < elem(later_index, 0)
  end

  defp restore_env(key, nil) when is_binary(key), do: System.delete_env(key)
  defp restore_env(key, value) when is_binary(key), do: System.put_env(key, value)
  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
