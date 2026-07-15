defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.Dev.Bootstrap.Single, as: BootstrapSingle
  alias Favn.Dev.ConsumerConfigTransport
  alias Favn.Dev.EnvBootstrap
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.RuntimeLaunch
  alias Favn.Dev.State
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Column
  alias Favn.SQL.Relation
  alias Favn.SQL.Result
  alias Mix.Tasks.Favn.Backfill, as: BackfillTask
  alias Mix.Tasks.Favn.Bootstrap.Single, as: BootstrapSingleTask
  alias Mix.Tasks.Favn.Build.Orchestrator, as: BuildOrchestratorTask
  alias Mix.Tasks.Favn.Build.Runner, as: BuildRunnerTask
  alias Mix.Tasks.Favn.Build.Single, as: BuildSingleTask
  alias Mix.Tasks.Favn.Build.Web, as: BuildWebTask
  alias Mix.Tasks.Favn.Dev, as: DevTask
  alias Mix.Tasks.Favn.Dev.Configured, as: ConfiguredDevTask
  alias Mix.Tasks.Favn.Diagnostics, as: DiagnosticsTask
  alias Mix.Tasks.Favn.Doctor, as: DoctorTask
  alias Mix.Tasks.Favn.Init, as: InitTask
  alias Mix.Tasks.Favn.Install, as: InstallTask
  alias Mix.Tasks.Favn.Inspect, as: InspectTask
  alias Mix.Tasks.Favn.Logs, as: LogsTask
  alias Mix.Tasks.Favn.Query, as: QueryTask
  alias Mix.Tasks.Favn.Reload, as: ReloadTask
  alias Mix.Tasks.Favn.Reload.Configured, as: ConfiguredReloadTask
  alias Mix.Tasks.Favn.Reset, as: ResetTask
  alias Mix.Tasks.Favn.Run, as: RunTask
  alias Mix.Tasks.Favn.Runs, as: RunsTask
  alias Mix.Tasks.Favn.Status, as: StatusTask
  alias Mix.Tasks.Favn.Stop, as: StopTask

  defmodule QueryConnection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Definition{
        name: :query_test,
        adapter: Mix.Tasks.Favn.PublicTasksTest.QueryAdapter,
        config_schema: [%{key: :database, type: :string, required: true}]
      }
    end
  end

  defmodule QueryAdapter do
    @behaviour Favn.SQL.Adapter

    @impl true
    def connect(%Resolved{}, _opts), do: {:ok, :query_conn}

    @impl true
    def disconnect(:query_conn, _opts), do: :ok

    @impl true
    def poolable?(%Resolved{}, _opts), do: true

    @impl true
    def validate_session(:query_conn, _opts), do: :ok

    @impl true
    def reset_session(:query_conn, %Resolved{}, _opts), do: :ok

    @impl true
    def capabilities(%Resolved{}, _opts), do: {:ok, %Capabilities{}}

    @impl true
    def execute(:query_conn, _statement, _opts), do: {:ok, %Result{kind: :execute}}

    @impl true
    def query(:query_conn, statement, _opts) do
      {:ok,
       %Result{
         kind: :query,
         columns: ["value"],
         rows: [%{"value" => IO.iodata_to_binary(statement)}]
       }}
    end

    @impl true
    def introspection_query(_kind, _payload, _opts), do: {:ok, "select 1"}

    @impl true
    def materialization_statements(%Favn.SQL.WritePlan{}, %Capabilities{}, _opts), do: {:ok, []}

    @impl true
    def relation(:query_conn, relation_ref, _opts) do
      {:ok,
       %Relation{name: relation_ref.name, schema: relation_ref.schema || "main", type: :table}}
    end

    @impl true
    def columns(:query_conn, _relation_ref, _opts) do
      {:ok, [%Column{name: "id", data_type: "integer"}]}
    end

    @impl true
    def row_count(:query_conn, _relation_ref, _opts), do: {:ok, 1}

    @impl true
    def sample(:query_conn, _relation_ref, _opts) do
      {:ok, %Result{kind: :query, columns: ["id"], rows: [%{"id" => 1}]}}
    end

    @impl true
    def table_metadata(:query_conn, _relation_ref, _opts) do
      {:ok, %{partitions: [%{month: "2026-05"}]}}
    end
  end

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_public_tasks_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_view"))

    File.write!(Path.join(root_dir, "mix.lock"), "lock")

    File.write!(
      Path.join(root_dir, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(root_dir, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    File.write!(Path.join(root_dir, "apps/favn_view/mix.exs"), "defmodule View.MixProject do end")

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "mix favn.dev raises when stack is already running", %{root_dir: root_dir} do
    current_pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "services" => %{
        "web" => %{"pid" => current_pid},
        "orchestrator" => %{"pid" => current_pid},
        "runner" => %{"pid" => current_pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    capture_io(fn ->
      assert_raise Mix.Error, ~r/local stack already running/, fn ->
        run_configured_dev(["--root-dir", root_dir])
      end
    end)
  end

  test "mix favn.dev raises when install is missing", %{root_dir: root_dir} do
    output =
      capture_io(fn ->
        assert_raise Mix.Error, ~r/install required; run mix favn.install/, fn ->
          run_configured_dev(["--root-dir", root_dir])
        end
      end)

    assert output =~ "Favn dev: checking local state"
  end

  test "mix favn.dev loads consumer runtime config before transport", %{root_dir: root_dir} do
    consumer_dir = Path.join(root_dir, "consumer")
    config_dir = Path.join(consumer_dir, "config")
    code_path = :code.get_path()
    previous_connections = Application.get_env(:favn, :connections)
    previous_runtime_mode = System.get_env("FAVN_DEV_RUNTIME_CONFIG_MODE")

    File.mkdir_p!(config_dir)

    File.write!(
      Path.join(consumer_dir, "mix.exs"),
      """
      defmodule FavnDevRuntimeConfigConsumer.MixProject do
        use Mix.Project

        def project do
          [app: :favn_dev_runtime_config_consumer, version: "0.1.0", elixir: "~> 1.20"]
        end

        def application, do: []
      end
      """
    )

    File.write!(
      Path.join(config_dir, "runtime.exs"),
      """
      import Config

      database =
        case System.fetch_env!("FAVN_DEV_RUNTIME_CONFIG_MODE") do
          "cloud" -> "cloud.duckdb"
          "local" -> "local.duckdb"
        end

      config :favn, :connections,
        ducklake: [
          open: [database: database],
          duckdb: [extensions: ["ducklake"]]
        ]
      """
    )

    File.write!(Path.join(consumer_dir, ".env"), "FAVN_DEV_RUNTIME_CONFIG_MODE=cloud\n")

    Application.delete_env(:favn, :connections)
    System.delete_env("FAVN_DEV_RUNTIME_CONFIG_MODE")

    on_exit(fn ->
      restore_env(:connections, previous_connections)

      if previous_runtime_mode do
        System.put_env("FAVN_DEV_RUNTIME_CONFIG_MODE", previous_runtime_mode)
      else
        System.delete_env("FAVN_DEV_RUNTIME_CONFIG_MODE")
      end

      Mix.Task.reenable("app.config")
      Mix.Task.reenable("compile")
    end)

    try do
      Mix.Project.in_project(:favn_dev_runtime_config_consumer, consumer_dir, fn _project ->
        assert Mix.Task.requirements(DevTask) == ["loadpaths"]
        assert Mix.Task.requirements(ConfiguredDevTask) == ["app.config"]
        assert Mix.Task.requirements(ReloadTask) == ["loadpaths"]
        assert Mix.Task.requirements(ConfiguredReloadTask) == ["app.config"]

        args = ["--root-dir", consumer_dir]
        assert :ok = EnvBootstrap.install_for_current_process(:dev, DevTask.parse_args(args))

        Mix.Task.reenable("app.config")
        Mix.Task.reenable("compile")

        capture_io(fn -> Mix.Tasks.App.Config.run([]) end)

        assert true = :code.set_path(code_path)
        assert Code.ensure_loaded?(Favn.Dev)
        assert Code.ensure_loaded?(ConsumerConfigTransport)

        assert {:ok, configured_opts} =
                 EnvBootstrap.consume(:dev, DevTask.parse_args(args))

        configured_opts =
          Keyword.put(configured_opts, :local_distribution,
            localhost: fn -> ~c"testhost.localdomain" end,
            resolver: fn ~c"testhost" -> {:ok, [{127, 0, 1, 1}]} end,
            epmd_executable: false
          )

        runner =
          RuntimeLaunch.runner_spec(
            %{"runner_root" => consumer_dir},
            configured_opts,
            %{runner_short: "favn_runner_runtime_config_test"},
            %{"rpc_cookie" => "cookie"}
          )

        assert runner.env["FAVN_DEV_RUNTIME_CONFIG_MODE"] == "cloud"

        encoded = runner.env["FAVN_DEV_CONSUMER_FAVN_CONFIG"]

        assert {:ok, config} = ConsumerConfigTransport.decode(encoded)
        connection = config |> Keyword.fetch!(:connections) |> Keyword.fetch!(:ducklake)

        assert connection[:open] == [database: "cloud.duckdb"]
        assert connection[:duckdb] == [extensions: ["ducklake"]]
      end)
    after
      _ = :code.set_path(code_path)
    end

    assert :code.get_path() == code_path
  end

  test "Dev.dev stays quiet unless progress callback is provided", %{root_dir: root_dir} do
    output =
      capture_io(fn ->
        assert {:error, :install_required} = Favn.Dev.dev(root_dir: root_dir)
      end)

    assert output == ""
  end

  test "Dev.dev reports progress through explicit callback", %{root_dir: root_dir} do
    caller = self()

    assert {:error, :install_required} =
             Favn.Dev.dev(root_dir: root_dir, progress_fun: &send(caller, {:progress, &1}))

    assert_received {:progress, "Favn dev: checking local state"}
  end

  test "mix favn.dev parses scheduler flags" do
    assert Keyword.get(DevTask.parse_args(["--scheduler"]), :scheduler) == true
    assert Keyword.get(DevTask.parse_args(["--no-scheduler"]), :scheduler) == false
    assert Keyword.get(DevTask.parse_args([]), :scheduler) == nil
  end

  test "no-positional public mix favn tasks reject invalid options and unexpected args" do
    tasks = [
      {BuildOrchestratorTask, "favn.build.orchestrator"},
      {BuildRunnerTask, "favn.build.runner"},
      {BuildSingleTask, "favn.build.single"},
      {BuildWebTask, "favn.build.web"},
      {BootstrapSingleTask, "favn.bootstrap.single"},
      {DiagnosticsTask, "favn.diagnostics"},
      {DevTask, "favn.dev"},
      {InstallTask, "favn.install"},
      {ReloadTask, "favn.reload"},
      {ResetTask, "favn.reset"},
      {StatusTask, "favn.status"},
      {StopTask, "favn.stop"}
    ]

    for {task, task_name} <- tasks do
      assert_raise Mix.Error, ~r/invalid option for mix #{Regex.escape(task_name)}/, fn ->
        task.run(["--bad-option"])
      end

      assert_raise Mix.Error, ~r/unexpected argument for mix #{Regex.escape(task_name)}/, fn ->
        task.run(["extra"])
      end
    end

    assert_raise Mix.Error, ~r/invalid option for mix favn.logs/, fn ->
      LogsTask.run(["--bad-option"])
    end
  end

  test "mix favn.bootstrap.single requires activation inputs by default" do
    previous_env = bootstrap_env()
    clear_bootstrap_env()
    on_exit(fn -> restore_bootstrap_env(previous_env) end)

    assert_raise Mix.Error,
                 ~r/missing required option\(s\): --manifest, --orchestrator-url, --service-token, --operator-username, --operator-password/,
                 fn -> BootstrapSingleTask.parse_args([]) end
  end

  test "mix favn.bootstrap.single reads env defaults and lets flags win" do
    previous_env = bootstrap_env()
    clear_bootstrap_env()

    System.put_env("FAVN_BOOTSTRAP_MANIFEST_PATH", "/env/manifest.json")
    System.put_env("FAVN_VIEW_ORCHESTRATOR_BASE_URL", "http://127.0.0.1:4000")
    System.put_env("FAVN_VIEW_ORCHESTRATOR_SERVICE_TOKEN", "env-token")
    System.put_env("FAVN_BOOTSTRAP_OPERATOR_USERNAME", "env-admin")
    System.put_env("FAVN_BOOTSTRAP_OPERATOR_PASSWORD", "env-password-long")

    on_exit(fn -> restore_bootstrap_env(previous_env) end)

    opts = BootstrapSingleTask.parse_args(["--manifest", "/flag/manifest.json"])

    assert Keyword.fetch!(opts, :manifest_path) == "/flag/manifest.json"
    assert Keyword.fetch!(opts, :orchestrator_url) == "http://127.0.0.1:4000"
    assert Keyword.fetch!(opts, :service_token) == "env-token"
    assert Keyword.fetch!(opts, :operator_username) == "env-admin"
    assert Keyword.fetch!(opts, :operator_password) == "env-password-long"
  end

  test "mix favn.bootstrap.single parses activation flag" do
    opts =
      BootstrapSingleTask.parse_args([
        "--manifest",
        "/tmp/manifest.json",
        "--orchestrator-url",
        "http://127.0.0.1:4101",
        "--service-token",
        "token",
        "--no-activate"
      ])

    assert Keyword.fetch!(opts, :activate?) == false
  end

  test "mix favn.diagnostics prints service-authenticated operator diagnostics", %{
    root_dir: root_dir
  } do
    {:ok, base_url, _server} = start_bootstrap_server(:ok)
    current_pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "orchestrator_base_url" => base_url,
      "services" => %{
        "web" => %{"pid" => 999_997},
        "orchestrator" => %{"pid" => current_pid},
        "runner" => %{"pid" => 999_998}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        DiagnosticsTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn operator diagnostics"
    assert output =~ "status: degraded"
    assert output =~ "storage_readiness: ok"
    assert output =~ "runner: error"
    assert output =~ "runner_not_available"
    refute output =~ "diagnostics-service-token"
  end

  test "mix favn.diagnostics --json prints machine-readable diagnostics", %{root_dir: root_dir} do
    {:ok, base_url, _server} = start_bootstrap_server(:ok)
    current_pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "orchestrator_base_url" => base_url,
      "services" => %{
        "web" => %{"pid" => current_pid},
        "orchestrator" => %{"pid" => current_pid},
        "runner" => %{"pid" => current_pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        DiagnosticsTask.run(["--root-dir", root_dir, "--json"])
      end)

    assert %{"status" => "degraded", "checks" => checks} = JSON.decode!(output)
    assert Enum.any?(checks, &(&1["check"] == "storage_readiness"))
  end

  test "mix favn.bootstrap.single prints matched active-manifest verification", %{
    root_dir: root_dir
  } do
    manifest_path = write_bootstrap_manifest(root_dir)
    {:ok, version} = BootstrapSingle.read_manifest_version(manifest_path)
    {:ok, base_url, _server} = start_bootstrap_server({:matched, version.manifest_version_id})

    output =
      capture_io(fn ->
        BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
      end)

    assert output =~ "Favn single-node bootstrap complete"
    assert output =~ "manifest registration: accepted"
    assert output =~ "runner registration: accepted"
    assert output =~ "active manifest verification: matched"
  end

  test "mix favn.bootstrap.single reports missing manifest files clearly", %{root_dir: root_dir} do
    missing_path = Path.join(root_dir, "missing_manifest.json")
    {:ok, base_url, _server} = start_bootstrap_server(:ok)

    assert_raise Mix.Error, ~r/bootstrap failed: manifest file not found:/, fn ->
      BootstrapSingleTask.run(bootstrap_args(missing_path, base_url))
    end
  end

  test "mix favn.bootstrap.single reports invalid service token clearly", %{root_dir: root_dir} do
    manifest_path = write_bootstrap_manifest(root_dir)
    {:ok, base_url, _server} = start_bootstrap_server(:invalid_service_token)

    assert_raise Mix.Error,
                 ~r/bootstrap failed: service token was rejected by orchestrator/,
                 fn ->
                   BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
                 end
  end

  test "mix favn.bootstrap.single reports manifest conflicts clearly", %{root_dir: root_dir} do
    manifest_path = write_bootstrap_manifest(root_dir)
    {:ok, base_url, _server} = start_bootstrap_server(:manifest_conflict)

    assert_raise Mix.Error, ~r/bootstrap failed: manifest version conflict/, fn ->
      BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
    end
  end

  test "mix favn.bootstrap.single safely prints tuple active-manifest verification", %{
    root_dir: root_dir
  } do
    manifest_path = write_bootstrap_manifest(root_dir)

    for verification <- [:skipped, :mismatch] do
      {:ok, base_url, _server} = start_bootstrap_server({verification, "mv_other"})

      output =
        capture_io(fn ->
          BootstrapSingleTask.run(bootstrap_args(manifest_path, base_url))
        end)

      assert output =~ "Favn single-node bootstrap complete"
      assert output =~ "active manifest verification: {:#{verification},"
    end
  end

  test "mix favn.dev raises with partial runtime recovery guidance", %{root_dir: root_dir} do
    log_path = Path.join(root_dir, ".favn/logs/runner.log")
    assert :ok = File.mkdir_p(Path.dirname(log_path))

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "sleep 30"],
      cwd: root_dir,
      log_path: log_path,
      env: %{}
    }

    assert {:ok, info} = DevProcess.start_service(spec)

    runtime = %{
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => info.pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    try do
      capture_io(fn ->
        assert_raise Mix.Error, ~r/local stack is in a partial\/dead state/, fn ->
          run_configured_dev(["--root-dir", root_dir])
        end
      end)
    after
      _ = DevProcess.stop_pid(info.pid)
      _ = State.clear_runtime(root_dir: root_dir)
    end
  end

  test "mix favn.status prints stack details", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "storage" => "memory",
      "active_manifest_version_id" => "mv_task_test",
      "orchestrator_base_url" => "http://127.0.0.1:4101",
      "web_base_url" => "http://127.0.0.1:4173",
      "node_names" => %{
        "runner" => "favn_runner_task@localhost",
        "orchestrator" => "favn_orchestrator_task@localhost",
        "control" => "favn_local_ctl_task@localhost"
      },
      "distribution_ports" => %{
        "runner" => 45_101,
        "orchestrator" => 45_102,
        "control" => 45_103
      },
      "services" => %{
        "operator" => %{
          "pid" => pid,
          "node_name" => "favn_orchestrator_task@localhost",
          "distribution_port" => 45_102
        },
        "runner" => %{
          "pid" => pid,
          "node_name" => "favn_runner_task@localhost",
          "distribution_port" => 45_101
        }
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        StatusTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local dev stack"
    assert output =~ "manifest: mv_task_test"
    assert output =~ "operator: running pid=#{pid}"
    assert output =~ "local URLs:"
    assert output =~ "web: url=http://127.0.0.1:4173"
    assert output =~ "orchestrator API: url=http://127.0.0.1:4101"
    assert output =~ "internal control plane:"

    assert output =~
             "runner node: running pid=#{pid} node=favn_runner_task@localhost distribution_port=45101"

    assert output =~
             "operator node: running pid=#{pid} node=favn_orchestrator_task@localhost distribution_port=45102"

    assert output =~ "control node: node=favn_local_ctl_task@localhost distribution_port=45103"
  end

  test "mix favn.status reports stale runtime hint", %{root_dir: root_dir} do
    runtime = %{
      "storage" => "memory",
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        StatusTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "status: stale"
    assert output =~ "hint: run mix favn.stop to clear stale runtime state"
  end

  test "mix favn.run requires a target" do
    assert_raise Mix.Error, ~r/missing target/, fn ->
      RunTask.run([])
    end
  end

  test "mix favn.run reports stopped local stack", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      RunTask.run(["Example.Pipeline", "--root-dir", root_dir])
    end
  end

  test "mix favn.run validates numeric options" do
    assert_raise Mix.Error, ~r/--timeout-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--timeout-ms", "0"])
    end

    assert_raise Mix.Error, ~r/--wait-timeout-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--wait-timeout-ms", "0"])
    end

    assert_raise Mix.Error, ~r/--run-timeout-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--run-timeout-ms", "0"])
    end

    assert_raise Mix.Error, ~r/--poll-interval-ms must be greater than 0/, fn ->
      RunTask.run(["Example.Pipeline", "--poll-interval-ms", "0"])
    end
  end

  test "mix favn.run reports terminal execution timeout with run error" do
    message =
      RunTask.terminal_run_error_message(%{
        "id" => "run_1",
        "status" => "timed_out",
        "error" => "asset timed out after 5000ms"
      })

    assert message =~ "run finished with status timed_out (run execution timeout)"
    assert message =~ "asset timed out after 5000ms"
  end

  test "mix favn.run formats structured run errors from the API" do
    error = %{
      "kind" => "error",
      "message" => "IO Error: Cannot open file \".data/ducklake/session/favn.duckdb\"",
      "reason" => "fallback reason",
      "redacted" => true
    }

    assert RunTask.format_run_error(error) ==
             "IO Error: Cannot open file \".data/ducklake/session/favn.duckdb\""

    message =
      RunTask.terminal_run_error_message(%{
        "id" => "run_1",
        "status" => "error",
        "error" => error
      })

    assert message ==
             "run finished with status error: IO Error: Cannot open file \".data/ducklake/session/favn.duckdb\""
  end

  test "mix favn.run includes structured HTTP error details" do
    message =
      RunTask.format_orchestrator_reason(
        {:http_error, 400,
         %{
           "error" => %{
             "message" => "Request failed",
             "details" => %{"reason" => ":audit_write_failed"}
           }
         }}
      )

    assert message == "HTTP 400: Request failed (reason: :audit_write_failed)"
  end

  test "mix favn.reload gives stale-run recovery instructions for in-flight runs" do
    message = ReloadTask.in_flight_runs_message(["run_pending", "run_running"])

    assert message =~ "reload blocked: in-flight runs exist"
    assert message =~ "mix favn.runs cancel RUN_ID"
    assert message =~ "if these runs are stale"
    assert message =~ "mix favn.reset"
  end

  test "configured lifecycle tasks reject direct invocation" do
    previous_token = System.get_env("FAVN_INTERNAL_ENV_BOOTSTRAP")
    System.delete_env("FAVN_INTERNAL_ENV_BOOTSTRAP")

    on_exit(fn ->
      if previous_token do
        System.put_env("FAVN_INTERNAL_ENV_BOOTSTRAP", previous_token)
      else
        System.delete_env("FAVN_INTERNAL_ENV_BOOTSTRAP")
      end
    end)

    assert_raise Mix.Error, ~r/internal task; run mix favn.dev/, fn ->
      ConfiguredDevTask.run([])
    end

    assert_raise Mix.Error, ~r/internal task; run mix favn.reload/, fn ->
      ConfiguredReloadTask.run([])
    end
  end

  test "mix favn.backfill parses submit command" do
    assert {:ok, {:submit, "Example.Pipeline", opts}} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--coverage-baseline-id",
               "baseline_1",
               "--no-wait"
             ])

    assert Keyword.fetch!(opts, :from) == "2026-04-01"
    assert Keyword.fetch!(opts, :to) == "2026-04-07"
    assert Keyword.fetch!(opts, :kind) == "day"
    assert Keyword.fetch!(opts, :timezone) == "Etc/UTC"
    assert Keyword.fetch!(opts, :coverage_baseline_id) == "baseline_1"
    assert Keyword.fetch!(opts, :wait) == false

    assert {:ok, {:submit, "Example.Pipeline", opts}} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--window",
               "month:2025-05..2026-05",
               "--dry-run"
             ])

    assert Keyword.fetch!(opts, :window) == "month:2025-05..2026-05"
    assert Keyword.fetch!(opts, :dry_run) == true
    assert Keyword.fetch!(opts, :timezone) == "Etc/UTC"
  end

  test "mix favn.backfill validates submit arguments" do
    assert {:error, message} = BackfillTask.parse_args(["submit", "Example.Pipeline"])
    assert message =~ "missing required option(s): --from, --to, --kind"

    assert {:ok, {:submit, "Example.Pipeline", _opts}} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--window",
               "day:2026-01-01..2026-01-02"
             ])

    assert {:error, message} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--window",
               "month:2025-05..2026-05",
               "--from",
               "2025-05-01"
             ])

    assert message == "--window cannot be combined with --from, --to, or --kind"

    assert {:error, message} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--bad"
             ])

    assert message == "invalid option for mix favn.backfill submit"

    assert {:error, message} =
             BackfillTask.parse_args([
               "submit",
               "Example.Pipeline",
               "--from",
               "2026-04-01",
               "--to",
               "2026-04-07",
               "--kind",
               "day",
               "--lookback",
               "2"
             ])

    assert message == "invalid option for mix favn.backfill submit"
  end

  test "mix favn.backfill parses read and rerun commands" do
    assert {:ok, {:windows, "run_1", opts}} =
             BackfillTask.parse_args([
               "windows",
               "run_1",
               "--status",
               "failed",
               "--limit",
               "50",
               "--offset",
               "100"
             ])

    assert Keyword.fetch!(opts, :status) == "failed"
    assert Keyword.fetch!(opts, :limit) == 50
    assert Keyword.fetch!(opts, :offset) == 100

    assert {:ok, {:coverage_baselines, opts}} =
             BackfillTask.parse_args([
               "coverage-baselines",
               "--pipeline-module",
               "Example.Pipeline"
             ])

    assert Keyword.fetch!(opts, :pipeline_module) == "Example.Pipeline"

    assert {:ok, {:asset_window_states, opts}} =
             BackfillTask.parse_args([
               "asset-window-states",
               "--asset-ref-module",
               "Example.Asset",
               "--asset-ref-name",
               "asset"
             ])

    assert Keyword.fetch!(opts, :asset_ref_module) == "Example.Asset"
    assert Keyword.fetch!(opts, :asset_ref_name) == "asset"

    assert {:ok, {:rerun_window, "run_1", opts}} =
             BackfillTask.parse_args(["rerun-window", "run_1", "--window-key", "day:2026-04-01"])

    assert Keyword.fetch!(opts, :window_key) == "day:2026-04-01"

    assert {:ok, {:repair, opts}} =
             BackfillTask.parse_args([
               "repair",
               "--pipeline-module",
               "Example.Pipeline",
               "--apply"
             ])

    assert Keyword.fetch!(opts, :pipeline_module) == "Example.Pipeline"
    assert Keyword.fetch!(opts, :apply) == true
  end

  test "mix favn.backfill validates repair arguments" do
    assert {:error, message} =
             BackfillTask.parse_args([
               "repair",
               "--backfill-run-id",
               "run_1",
               "--pipeline-module",
               "Example.Pipeline"
             ])

    assert message ==
             "expected at most one repair scope: --all, --backfill-run-id, or --pipeline-module"

    assert {:error, message} = BackfillTask.parse_args(["repair", "extra"])
    assert message == "unexpected argument for mix favn.backfill repair"
  end

  test "mix favn.backfill reports stopped local stack", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      BackfillTask.run([
        "submit",
        "Example.Pipeline",
        "--from",
        "2026-04-01",
        "--to",
        "2026-04-07",
        "--kind",
        "day",
        "--root-dir",
        root_dir
      ])
    end
  end

  test "mix favn.install writes install state", %{root_dir: root_dir} do
    output =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    assert output =~ "Favn install complete"
    assert {:ok, _install} = State.read_install(root_dir: root_dir)
    assert {:ok, _toolchain} = State.read_toolchain(root_dir: root_dir)
  end

  test "mix favn.init requires duckdb sample flags" do
    assert_raise Mix.Error, ~r/missing required option\(s\): --duckdb, --sample/, fn ->
      InitTask.run([])
    end
  end

  test "mix favn.init rejects root-dir because it targets the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/usage: mix favn.init --duckdb --sample/, fn ->
      InitTask.run(["--root-dir", root_dir, "--duckdb", "--sample"])
    end
  end

  test "mix favn.init prints generated bootstrap summary", %{root_dir: root_dir} do
    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule PublicTaskSample.MixProject do
        use Mix.Project

        def project do
          [app: :public_task_sample, version: "0.1.0", deps: deps()]
        end

        def application do
          []
        end

        defp deps do
          [
            {:favn, path: "../favn/apps/favn"}
          ]
        end
      end
      """
    )

    output =
      File.cd!(root_dir, fn ->
        capture_io(fn ->
          InitTask.run(["--duckdb", "--sample"])
        end)
      end)

    assert output =~ "Favn local bootstrap complete"
    assert output =~ "pipeline: Favn.Pipelines.LocalSmoke"
    assert File.exists?(Path.join(root_dir, "lib/favn/pipelines/local_smoke.ex"))
  end

  test "mix favn.doctor rejects root-dir because it checks the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/usage: mix favn.doctor/, fn ->
      DoctorTask.run(["--root-dir", root_dir, "--skip-compile"])
    end
  end

  test "mix favn.install reports already up to date", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    output =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install", "--skip-tool-checks"])
      end)

    assert output =~ "Favn install is already up to date"
  end

  test "mix favn.dev reports runtime compile failures under root_dir", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      Application.put_env(:favn, :local,
        orchestrator_port: free_port(),
        web_port: free_port()
      )

      capture_io(fn ->
        assert_raise Mix.Error,
                     ~r/(runtime compile failed for runtime_root under --root-dir|local Erlang shortname host is unavailable|local Erlang shortname host .* must resolve to a loopback 127\.\* address|port conflict: .* cannot bind port)/,
                     fn ->
                       run_configured_dev(["--root-dir", root_dir])
                     end
      end)
    after
      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.install no longer requires Node tooling", %{root_dir: root_dir} do
    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      output =
        capture_io(fn ->
          InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
        end)

      assert output =~ "Favn install complete"
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end

  test "mix favn.logs prints service logs", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert :ok = File.write(Path.join(root_dir, ".favn/logs/web.log"), "hello\n")

    output =
      capture_io(fn ->
        LogsTask.run(["--root-dir", root_dir, "--service", "web"])
      end)

    assert output =~ "hello"
  end

  test "mix favn.logs accepts a run id for event output" do
    assert {:ok, {:run_events, "run_1", opts}} = LogsTask.parse_args(["run_1", "--tail", "10"])
    assert Keyword.fetch!(opts, :tail) == 10

    assert {:error, message} = LogsTask.parse_args(["run_1", "--follow"])
    assert message =~ "RUN_ID cannot be combined"
  end

  test "mix favn.runs parses list show and cancel subcommands" do
    assert {:ok, {:list, opts}} =
             RunsTask.parse_args(["list", "--status", "error", "--limit", "5"])

    assert Keyword.fetch!(opts, :status) == "error"
    assert Keyword.fetch!(opts, :limit) == 5

    assert {:ok, {:show, "run_1", []}} = RunsTask.parse_args(["show", "run_1"])

    assert {:ok, {:cancel, "run_1", opts}} =
             RunsTask.parse_args([
               "cancel",
               "run_1",
               "--wait",
               "--wait-timeout-ms",
               "5000",
               "--poll-interval-ms",
               "100"
             ])

    assert Keyword.fetch!(opts, :wait) == true
    assert Keyword.fetch!(opts, :wait_timeout_ms) == 5000
    assert Keyword.fetch!(opts, :poll_interval_ms) == 100

    assert {:error, message} = RunsTask.parse_args([])
    assert message =~ "missing subcommand"
  end

  test "mix favn.inspect parses relation and partition inspection commands" do
    assert {:ok, {"relation", "raw.sales.orders", opts}} =
             InspectTask.parse_args(["relation", "raw.sales.orders", "--connection", "warehouse"])

    assert Keyword.fetch!(opts, :connection) == "warehouse"

    assert {:ok, {"partitions", "raw.sales.orders", []}} =
             InspectTask.parse_args(["partitions", "raw.sales.orders"])

    assert {:error, usage_message} = InspectTask.parse_args(["relation"])
    assert usage_message =~ "usage: mix favn.inspect relation RELATION"

    assert {:error, invalid_message} = InspectTask.parse_args(["relation", "--bad-option"])
    assert invalid_message == "invalid option for mix favn.inspect relation"
  end

  test "mix favn.query parses SQL query arguments" do
    assert {:ok, {"select 1", opts}} =
             QueryTask.parse_args(["select 1", "--connection", "warehouse", "--limit", "5"])

    assert Keyword.fetch!(opts, :connection) == "warehouse"
    assert Keyword.fetch!(opts, :limit) == 5

    assert {:error, usage_message} = QueryTask.parse_args([])
    assert usage_message =~ "usage: mix favn.query"

    assert {:error, invalid_message} = QueryTask.parse_args(["select 1", "--bad-option"])
    assert invalid_message == "invalid option for mix favn.query"
  end

  test "mix favn.query starts the SQL runtime before connecting" do
    configure_query_connection!()
    stop_sql_runtime!()

    output =
      capture_io(fn ->
        QueryTask.run(["select 1", "--connection", "query_test"])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "value"
    assert output =~ "select 1"
  end

  test "mix favn.inspect relation starts the SQL runtime before connecting" do
    configure_query_connection!()
    stop_sql_runtime!()

    output =
      capture_io(fn ->
        InspectTask.run(["relation", "raw.sales.orders", "--connection", "query_test"])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "relation: query_test.raw.sales.orders"
    assert output =~ "row_count: 1"
    assert output =~ "id\tinteger"
  end

  test "mix favn.inspect partitions starts the SQL runtime before connecting" do
    configure_query_connection!()
    stop_sql_runtime!()

    output =
      capture_io(fn ->
        InspectTask.run(["partitions", "raw.sales.orders", "--connection", "query_test"])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "relation: query_test.raw.sales.orders"
    assert output =~ "partitions: [%{month: \"2026-05\"}]"
  end

  test "mix favn.reset removes .favn when stack is not running", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert File.dir?(Path.join(root_dir, ".favn"))

    output =
      capture_io(fn ->
        ResetTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local state reset complete"
    refute File.exists?(Path.join(root_dir, ".favn"))
  end

  test "mix favn.build.runner prints build summary" do
    _ =
      capture_io(fn ->
        InstallTask.run(["--skip-web-install", "--skip-runtime-deps-install"])
      end)

    output =
      capture_io(fn ->
        BuildRunnerTask.run([])
      end)

    assert output =~ "Favn runner build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/runner/"
  end

  test "mix favn.build.runner rejects root_dir outside current mix project", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    assert_raise Mix.Error,
                 ~r/runner build is rooted in the current Mix project only/,
                 fn ->
                   BuildRunnerTask.run(["--root-dir", root_dir])
                 end
  end

  test "mix favn.build.web prints build summary", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildWebTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn web build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/web/"
  end

  test "mix favn.build.web no longer requires Node tooling", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run([
          "--root-dir",
          root_dir,
          "--skip-web-install",
          "--skip-tool-checks"
        ])
      end)

    previous_path = System.get_env("PATH")

    try do
      System.put_env("PATH", "")

      output =
        capture_io(fn ->
          BuildWebTask.run(["--root-dir", root_dir])
        end)

      assert output =~ "Favn web build complete"
    after
      if previous_path, do: System.put_env("PATH", previous_path), else: System.delete_env("PATH")
    end
  end

  test "mix favn.dev reports port conflicts before startup", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      Application.put_env(:favn, :local, orchestrator_port: free_port(), web_port: port)

      capture_io(fn ->
        assert_raise Mix.Error,
                     ~r/port conflict: web cannot bind port #{port}; free the port and retry/,
                     fn ->
                       run_configured_dev(["--root-dir", root_dir])
                     end
      end)
    after
      :ok = :gen_tcp.close(socket)

      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.dev reports postgres connectivity failures", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    previous_local = Application.get_env(:favn, :local, :__missing__)

    try do
      web_port = free_port()

      Application.put_env(
        :favn,
        :local,
        storage: :postgres,
        orchestrator_port: free_port(),
        web_port: web_port,
        postgres: [
          hostname: "127.0.0.1",
          port: 1,
          username: "postgres",
          password: "postgres",
          database: "favn",
          ssl: false,
          pool_size: 10
        ]
      )

      capture_io(fn ->
        assert_raise Mix.Error, ~r/postgres unavailable at 127.0.0.1:1/, fn ->
          run_configured_dev(["--root-dir", root_dir])
        end
      end)
    after
      case previous_local do
        :__missing__ -> Application.delete_env(:favn, :local)
        value -> Application.put_env(:favn, :local, value)
      end
    end
  end

  test "mix favn.build.orchestrator prints build summary", %{root_dir: root_dir} do
    _ =
      capture_io(fn ->
        InstallTask.run(["--root-dir", root_dir, "--skip-web-install"])
      end)

    output =
      capture_io(fn ->
        BuildOrchestratorTask.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn orchestrator build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/orchestrator/"
  end

  test "mix favn.build.single prints build summary" do
    _ =
      capture_io(fn ->
        InstallTask.run(["--skip-web-install", "--skip-runtime-deps-install"])
      end)

    output =
      capture_io(fn ->
        BuildSingleTask.run(["--storage", "sqlite"])
      end)

    assert output =~ "Favn single build complete"
    assert output =~ "build id:"
    assert output =~ "/.favn/dist/single/"
  end

  defp write_bootstrap_manifest(root_dir) do
    path = Path.join(root_dir, "bootstrap_manifest.json")

    File.write!(
      path,
      JSON.encode_to_iodata!(%{
        schema_version: 4,
        runner_contract_version: 4,
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })
    )

    path
  end

  defp bootstrap_args(manifest_path, base_url) do
    [
      "--manifest",
      manifest_path,
      "--orchestrator-url",
      base_url,
      "--service-token",
      "token-1",
      "--operator-username",
      "admin",
      "--operator-password",
      "admin-password-long"
    ]
  end

  defp start_bootstrap_server(verification) do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: false, packet: :raw])
    {:ok, port} = :inet.port(socket)

    server =
      spawn_link(fn ->
        serve_bootstrap_requests(socket, verification, 6)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp serve_bootstrap_requests(socket, _verification, 0), do: :gen_tcp.close(socket)

  defp serve_bootstrap_requests(socket, verification, remaining) do
    case :gen_tcp.accept(socket, 2_000) do
      {:ok, client} ->
        {:ok, request} = :gen_tcp.recv(client, 0, 2_000)
        send_bootstrap_response(client, request, verification)
        :gen_tcp.close(client)
        serve_bootstrap_requests(socket, verification, remaining - 1)

      {:error, :timeout} ->
        :gen_tcp.close(socket)
    end
  end

  defp send_bootstrap_response(client, request, verification) do
    [request_line | _headers] = String.split(request, "\r\n", parts: 2)
    [_method, path, _version] = String.split(request_line, " ", parts: 3)

    cond do
      path == "/api/orchestrator/v1/bootstrap/service-token" ->
        send_service_token_response(client, verification)

      path == "/api/orchestrator/v1/diagnostics" ->
        send_json(client, 200, %{
          data: %{
            status: "degraded",
            generated_at: "2026-05-02T00:00:00Z",
            checks: [
              %{
                check: "storage_readiness",
                status: "ok",
                summary: "Storage is ready",
                details: %{}
              },
              %{
                check: "runner",
                status: "error",
                summary: "Runner is unavailable",
                reason: "runner_not_available",
                details: %{client: "Elixir.FavnOrchestrator.RunnerClient.LocalNode"}
              }
            ]
          }
        })

      path == "/api/orchestrator/v1/manifests" ->
        send_manifest_publish_response(client, verification)

      path == "/api/orchestrator/v1/auth/password/sessions" ->
        send_json(client, 201, %{
          data: %{
            session: %{id: "ses_1"},
            session_token: "raw_session_token_1",
            actor: %{id: "act_1"}
          }
        })

      path == "/api/orchestrator/v1/bootstrap/active-manifest" ->
        send_active_manifest_response(client, verification)

      String.contains?(path, "/api/orchestrator/v1/manifests/") and
          String.contains?(path, "/runner/register") ->
        send_json(client, 200, %{data: %{registration: %{status: "accepted"}}})

      String.contains?(path, "/api/orchestrator/v1/manifests/") and
          String.contains?(path, "/activate") ->
        send_json(client, 200, %{data: %{activated: true}})

      true ->
        send_json(client, 404, %{error: %{reason: "not_found"}})
    end
  end

  defp send_service_token_response(client, :invalid_service_token) do
    send_json(client, 401, %{error: %{code: "service_unauthorized"}})
  end

  defp send_service_token_response(client, _verification) do
    send_json(client, 200, %{data: %{status: "ok"}})
  end

  defp send_manifest_publish_response(client, :manifest_conflict) do
    send_json(client, 409, %{error: %{code: "manifest_conflict"}})
  end

  defp send_manifest_publish_response(client, _verification) do
    send_json(client, 200, %{
      data: %{manifest: %{}, registration: %{status: "accepted"}}
    })
  end

  defp send_active_manifest_response(client, {status, manifest_version_id})
       when status in [:matched, :mismatch] do
    send_json(client, 200, %{data: %{manifest_version_id: manifest_version_id}})
  end

  defp send_active_manifest_response(client, {:skipped, _reason}) do
    send_json(client, 503, %{error: %{reason: "unavailable"}})
  end

  defp send_json(client, status, payload) do
    body = JSON.encode!(payload)
    reason = if status == 200, do: "OK", else: "Error"

    :gen_tcp.send(client, [
      "HTTP/1.1 #{status} #{reason}\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n",
      "\r\n",
      body
    ])
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp run_configured_dev(args) do
    :ok = EnvBootstrap.install_for_current_process(:dev, DevTask.parse_args(args))
    DevTask.run_configured(args)
  end

  defp configure_query_connection! do
    previous_modules = Application.get_env(:favn, :connection_modules)
    previous_connections = Application.get_env(:favn, :connections)

    Application.put_env(:favn, :connection_modules, [QueryConnection])
    Application.put_env(:favn, :connections, query_test: [database: ":memory:"])

    on_exit(fn ->
      stop_sql_runtime!()
      restore_env(:connection_modules, previous_modules)
      restore_env(:connections, previous_connections)
    end)
  end

  defp stop_sql_runtime! do
    if Process.whereis(Favn.SQL.SessionPool) do
      _ = Favn.SQL.SessionPool.reset()
    end

    _ = Application.stop(:favn_sql_runtime)
    refute Process.whereis(Favn.SQL.SessionPool)
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)

  defp clear_bootstrap_env do
    bootstrap_env_names()
    |> Enum.each(&System.delete_env/1)
  end

  defp bootstrap_env do
    Map.new(bootstrap_env_names(), fn name -> {name, System.get_env(name)} end)
  end

  defp restore_bootstrap_env(env) when is_map(env) do
    Enum.each(env, fn
      {name, nil} -> System.delete_env(name)
      {name, value} -> System.put_env(name, value)
    end)
  end

  defp bootstrap_env_names do
    [
      "FAVN_BOOTSTRAP_MANIFEST_PATH",
      "FAVN_VIEW_ORCHESTRATOR_BASE_URL",
      "FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_VIEW_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_ORCHESTRATOR_SERVICE_TOKEN",
      "FAVN_BOOTSTRAP_OPERATOR_USERNAME",
      "FAVN_BOOTSTRAP_OPERATOR_PASSWORD",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME",
      "FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD"
    ]
  end
end
