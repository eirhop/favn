defmodule Favn.Dev.RuntimeLaunchTest do
  use ExUnit.Case, async: true

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

    runner = RuntimeLaunch.runner_spec(runtime, [], node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, [], secrets)

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
  end

  test "web spec carries local admin credentials from consumer dotenv" do
    root_dir = Path.join(System.tmp_dir!(), "favn_web_env_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    assert :ok = File.mkdir_p(root_dir)

    assert :ok =
             File.write(Path.join(root_dir, ".env"), """
             FAVN_WEB_ADMIN_USERNAME=admin
             FAVN_WEB_ADMIN_PASSWORD="admin-password"
             FAVN_WEB_ADMIN_SESSION_TTL_SECONDS=60 # optional inline comment
             IGNORED_ENV=ignored
             """)

    runtime = %{
      "web_root" => Path.join(root_dir, "web/favn_web")
    }

    config = Config.resolve([])
    secrets = %{"service_token" => "token", "web_session_secret" => "secret"}
    web = RuntimeLaunch.web_spec(runtime, config, [root_dir: root_dir], secrets)

    assert web.env["FAVN_WEB_ADMIN_USERNAME"] == "admin"
    assert web.env["FAVN_WEB_ADMIN_PASSWORD"] == "admin-password"
    assert web.env["FAVN_WEB_ADMIN_SESSION_TTL_SECONDS"] == "60"
    refute Map.has_key?(web.env, "IGNORED_ENV")
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

    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)
    code = eval_code!(orchestrator)

    assert orchestrator.env["FAVN_DEV_STORAGE"] == "memory"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES"] == "operator"
    assert code =~ ~s("memory" ->)
    assert code =~ "FavnOrchestrator.Storage.Adapter.Memory"
    assert code =~ "unsupported FAVN_DEV_STORAGE"
  end

  test "orchestrator spec configures storage before starting orchestrator" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve(storage: :sqlite)
    node_names = %{runner_full: "favn_runner_test@host", orchestrator_short: "favn_orchestrator_test"}
    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    code =
      runtime
      |> RuntimeLaunch.orchestrator_spec(config, [], node_names, secrets)
      |> eval_code!()

    assert before?(code, "Application.put_env(:favn_orchestrator, :storage_adapter", "Application.ensure_all_started(:favn_storage_sqlite)")
    assert before?(code, "Application.ensure_all_started(:favn_storage_sqlite)", "Application.ensure_all_started(:favn_orchestrator)")
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

    runner = RuntimeLaunch.runner_spec(runtime, [build_path: build_path], node_names, secrets)
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

    runner = RuntimeLaunch.runner_spec(runtime, [root_dir: "/tmp/consumer"], node_names, secrets)
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
    assert before?(code, "Application.put_env(:favn, key, value)", "Application.ensure_all_started(:favn_runner)")
  end

  test "orchestrator spec disables scheduler by default" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve([])
    node_names = %{runner_full: "favn_runner_test@host", orchestrator_short: "favn_orchestrator_test"}
    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)
    code = eval_code!(orchestrator)

    assert orchestrator.env["FAVN_DEV_SCHEDULER_ENABLED"] == "0"
    assert code =~ "FAVN_DEV_SCHEDULER_ENABLED"
    refute code =~ "enabled: true"
  end

  test "orchestrator spec enables scheduler when resolved config enables it" do
    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve(scheduler: true)
    node_names = %{runner_full: "favn_runner_test@host", orchestrator_short: "favn_orchestrator_test"}
    secrets = %{"rpc_cookie" => "cookie", "service_token" => "token"}

    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)

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

  defp before?(text, earlier, later) do
    earlier_index = :binary.match(text, earlier)
    later_index = :binary.match(text, later)

    match?({_, _}, earlier_index) and match?({_, _}, later_index) and elem(earlier_index, 0) < elem(later_index, 0)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
