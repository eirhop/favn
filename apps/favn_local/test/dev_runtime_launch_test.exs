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

    opts = [root_dir: root_dir]
    runner = RuntimeLaunch.runner_spec(runtime, opts, node_names, secrets)
    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, opts, node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, opts, secrets)
    runner_port = RuntimeLaunch.distribution_port(:runner, opts)
    orchestrator_port = RuntimeLaunch.distribution_port(:orchestrator, opts)
    runner_erl = erl_flag!(runner)
    orchestrator_erl = erl_flag!(orchestrator)
    code = eval_code!(orchestrator)

    assert runner_erl =~ "inet_dist_use_interface {127,0,0,1}"
    assert runner_erl =~ "inet_dist_listen_min #{runner_port}"
    assert runner_erl =~ "inet_dist_listen_max #{runner_port}"
    assert orchestrator_erl =~ "inet_dist_use_interface {127,0,0,1}"
    assert orchestrator_erl =~ "inet_dist_listen_min #{orchestrator_port}"
    assert orchestrator_erl =~ "inet_dist_listen_max #{orchestrator_port}"
    assert orchestrator.env["FAVN_ORCHESTRATOR_API_BIND_IP"] == "127.0.0.1"
    assert code =~ "bind_ip: api_bind_ip"
    assert web.args == [hd(web.args), "preview", "--host", "127.0.0.1", "--port", "4173"]
  end

  test "orchestrator spec carries bootstrap credentials from consumer dotenv and shell env" do
    root_dir = Path.join(System.tmp_dir!(), "favn_orchestrator_env_#{System.unique_integer([:positive])}")
    previous_env = snapshot_system_env(bootstrap_env_keys())

    Enum.each(bootstrap_env_keys(), &System.delete_env/1)
    System.put_env("FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", "shell-password")

    on_exit(fn ->
      restore_system_env(previous_env)
      File.rm_rf(root_dir)
    end)

    assert :ok = File.mkdir_p(root_dir)

    assert :ok =
             File.write(Path.join(root_dir, ".env"), """
             FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME=admin
             FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD="dotenv-password"
             FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME=Local Admin # inline comment
             FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES=admin,operator
             IGNORED_ENV=ignored
             """)

    runtime = %{
      "orchestrator_root" => Path.join(root_dir, "runtime"),
      "web_root" => Path.join(root_dir, "web/favn_web")
    }

    config = Config.resolve([])
    node_names = %{runner_full: "runner@host", orchestrator_short: "orchestrator"}

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "web_session_secret" => "secret",
      "local_operator_username" => "generated-user",
      "local_operator_password" => "generated-password"
    }

    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [root_dir: root_dir], node_names, secrets)
    web = RuntimeLaunch.web_spec(runtime, config, [root_dir: root_dir], secrets)

    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME"] == "admin"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD"] == "shell-password"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME"] == "Local Admin"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES"] == "admin,operator"
    refute Map.has_key?(web.env, "IGNORED_ENV")
    refute Map.has_key?(web.env, "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME")
    refute Map.has_key?(web.env, "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD")
  end

  test "orchestrator spec uses generated local operator fallback without bootstrap env" do
    previous_env = snapshot_system_env(bootstrap_env_keys())
    Enum.each(bootstrap_env_keys(), &System.delete_env/1)

    on_exit(fn ->
      restore_system_env(previous_env)
    end)

    runtime = %{"orchestrator_root" => "/tmp/favn_runtime"}
    config = Config.resolve([])
    node_names = %{runner_full: "runner@host", orchestrator_short: "orchestrator"}

    secrets = %{
      "rpc_cookie" => "cookie",
      "service_token" => "token",
      "local_operator_username" => "generated-user",
      "local_operator_password" => "generated-password"
    }

    orchestrator = RuntimeLaunch.orchestrator_spec(runtime, config, [], node_names, secrets)

    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME"] == "generated-user"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD"] == "generated-password"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME"] == "Favn Local Operator"
    assert orchestrator.env["FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES"] == "operator"
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

    match?({_, _}, earlier_index) and match?({_, _}, later_index) and elem(earlier_index, 0) < elem(later_index, 0)
  end

  defp restore_env(key, nil) when is_binary(key), do: System.delete_env(key)
  defp restore_env(key, value) when is_binary(key), do: System.put_env(key, value)
  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)

  defp bootstrap_env_keys do
    ~w(
      FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME
      FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD
      FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME
      FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES
    )
  end

  defp snapshot_system_env(keys) do
    Map.new(keys, fn key -> {key, System.get_env(key)} end)
  end

  defp restore_system_env(snapshot) do
    Enum.each(snapshot, fn {key, value} -> restore_env(key, value) end)
  end
end
