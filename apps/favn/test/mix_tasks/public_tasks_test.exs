defmodule Mix.Tasks.Favn.PublicTasksTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Favn.Connection.Definition
  alias Favn.Connection.Resolved
  alias Favn.Dev.EnvBootstrap
  alias Favn.Dev.State
  alias Favn.SQL.Capabilities
  alias Favn.SQL.Column
  alias Favn.SQL.Relation
  alias Favn.SQL.Result
  alias Mix.Tasks.Favn.Backfill, as: BackfillTask
  alias Mix.Tasks.Favn.Build.ControlPlane, as: BuildControlPlaneTask
  alias Mix.Tasks.Favn.Build.Runner, as: BuildRunnerTask
  alias Mix.Tasks.Favn.Dev, as: DevTask
  alias Mix.Tasks.Favn.Dev.Configured, as: ConfiguredDevTask
  alias Mix.Tasks.Favn.Diagnostics, as: DiagnosticsTask
  alias Mix.Tasks.Favn.Doctor, as: DoctorTask
  alias Mix.Tasks.Favn.Init, as: InitTask
  alias Mix.Tasks.Favn.Install, as: InstallTask
  alias Mix.Tasks.Favn.Inspect, as: InspectTask
  alias Mix.Tasks.Favn.Logs, as: LogsTask
  alias Mix.Tasks.Favn.Maintainer.Dev, as: MaintainerDevTask
  alias Mix.Tasks.Favn.Query, as: QueryTask
  alias Mix.Tasks.Favn.Rebuild, as: RebuildTask
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

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "mix favn.dev raises when install is missing", %{root_dir: root_dir} do
    compose_file = Path.join(root_dir, "deploy/compose.local.yml")
    File.mkdir_p!(Path.dirname(compose_file))
    File.write!(compose_file, "services: {}\n")

    capture_io(fn ->
      assert_raise Mix.Error, ~r/install required; run mix favn.install/, fn ->
        run_configured_dev(["--root-dir", root_dir])
      end
    end)
  end

  test "Dev.dev stays quiet unless progress callback is provided", %{root_dir: root_dir} do
    compose_file = Path.join(root_dir, "deploy/compose.local.yml")
    File.mkdir_p!(Path.dirname(compose_file))
    File.write!(compose_file, "services: {}\n")

    output =
      capture_io(fn ->
        assert {:error, :install_required} = Favn.Dev.dev(root_dir: root_dir)
      end)

    assert output == ""
  end

  test "mix favn.dev parses scheduler and Compose selection flags" do
    assert Keyword.get(DevTask.parse_args(["--scheduler"]), :scheduler) == true
    assert Keyword.get(DevTask.parse_args(["--no-scheduler"]), :scheduler) == false
    assert Keyword.get(DevTask.parse_args([]), :scheduler) == nil

    assert Keyword.fetch!(
             DevTask.parse_args(["--compose-file", "deploy/compose.team.yml"]),
             :compose_file
           ) == "deploy/compose.team.yml"
  end

  test "mix favn.maintainer.dev uses normal local Compose selection flags" do
    assert Keyword.get(MaintainerDevTask.parse_args(["--scheduler"]), :scheduler) == true

    assert Keyword.fetch!(
             MaintainerDevTask.parse_args(["--compose-file", "deploy/compose.team.yml"]),
             :compose_file
           ) == "deploy/compose.team.yml"
  end

  test "mix favn.build.runner rejects a caller-supplied maintainer token", %{
    root_dir: root_dir
  } do
    variable = "FAVN_INTERNAL_MAINTAINER_RUNNER_BUILD"
    System.put_env(variable, String.duplicate("a", 64))

    assert_raise Mix.Error, ~r/invalid_maintainer_runner_build/, fn ->
      BuildRunnerTask.run_build(root_dir: root_dir)
    end

    assert System.get_env(variable) == nil
  end

  test "no-positional public mix favn tasks reject invalid options and unexpected args" do
    tasks = [
      {BuildControlPlaneTask, "favn.build.control_plane"},
      {BuildRunnerTask, "favn.build.runner"},
      {DiagnosticsTask, "favn.diagnostics"},
      {DevTask, "favn.dev"},
      {InstallTask, "favn.install"},
      {MaintainerDevTask, "favn.maintainer.dev"},
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

  test "mix favn.run parses dependency and refresh switches", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      RunTask.run([
        "Example.Asset:asset",
        "--root-dir",
        root_dir,
        "--dependencies",
        "none",
        "--refresh",
        "force_selected"
      ])
    end
  end

  test "mix favn.run reports canonical dependency and refresh validation" do
    assert_raise Mix.Error, ~r/--dependencies must be one of: all, none/, fn ->
      RunTask.run(["Example.Asset:asset", "--dependencies", "upstream"])
    end

    assert_raise Mix.Error,
                 ~r/--refresh must be one of: auto, missing, force_selected, force_selected_upstream, force_all/,
                 fn ->
                   RunTask.run(["Example.Asset:asset", "--refresh", "sometimes"])
                 end

    assert RunTask.error_message(:dependencies_only_supported_for_assets) ==
             "--dependencies is only supported for asset targets"

    assert RunTask.error_message({:invalid_pipeline_refresh_mode, "force_selected"}) ==
             "pipeline --refresh must be one of: auto, missing, force_all"

    assert RunTask.error_message({:refresh_include_upstream_requires_dependencies, :all}) ==
             "--refresh force_selected_upstream requires --dependencies all"
  end

  test "mix favn.run documents targeted asset repair and safe defaults" do
    assert {:docs_v1, _, :elixir, "text/markdown", %{"en" => moduledoc}, _, _} =
             Code.fetch_docs(RunTask)

    assert moduledoc =~ "--dependencies none --refresh force_selected"
    assert moduledoc =~ "defaults remain dependency scope `all`"
    assert moduledoc =~ "use it only after confirming"
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

  test "mix favn.backfill separates missing-window plan and submission" do
    assert {:ok, {:missing_plan, "Example.Asset", opts}} =
             BackfillTask.parse_args([
               "missing-plan",
               "Example.Asset",
               "--plan-file",
               "coverage-plan.json",
               "--limit",
               "250"
             ])

    assert Keyword.fetch!(opts, :plan_file) == "coverage-plan.json"
    assert Keyword.fetch!(opts, :limit) == 250

    assert {:ok, {:missing_submit, "Example.Asset", opts}} =
             BackfillTask.parse_args([
               "missing-submit",
               "Example.Asset",
               "--plan-file",
               "coverage-plan.json"
             ])

    assert Keyword.fetch!(opts, :plan_file) == "coverage-plan.json"

    assert {:error, "missing required option: --plan-file"} =
             BackfillTask.parse_args(["missing-submit", "Example.Asset"])
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

  test "mix favn.rebuild keeps planning and approval separate" do
    assert {:ok, {:plan, "Example.Asset", plan_opts}} =
             RebuildTask.parse_args([
               "plan",
               "Example.Asset",
               "--reason",
               "schema changed"
             ])

    assert Keyword.fetch!(plan_opts, :reason) == "schema changed"

    plan_hash = String.duplicate("a", 64)

    assert {:ok, {:start, "rebuild_plan_1", start_opts}} =
             RebuildTask.parse_args([
               "start",
               "rebuild_plan_1",
               "--plan-hash",
               plan_hash
             ])

    assert Keyword.fetch!(start_opts, :plan_hash) == plan_hash
    assert {:ok, {:status, "rebuild_1", []}} = RebuildTask.parse_args(["status", "rebuild_1"])
    assert {:ok, {:retry, "rebuild_1", []}} = RebuildTask.parse_args(["retry", "rebuild_1"])

    assert {:ok, {:reconcile, "rebuild_1", []}} =
             RebuildTask.parse_args(["reconcile", "rebuild_1"])

    assert {:ok, {:cancel, "rebuild_1", cancel_opts}} =
             RebuildTask.parse_args([
               "cancel",
               "rebuild_1",
               "--reason",
               "operator request"
             ])

    assert Keyword.fetch!(cancel_opts, :reason) == "operator request"
  end

  test "mix favn.rebuild requires explicit reason and plan hash" do
    assert {:error, "missing required option: --reason"} =
             RebuildTask.parse_args(["plan", "Example.Asset"])

    assert {:error, "missing required option: --plan-hash"} =
             RebuildTask.parse_args(["start", "rebuild_plan_1"])

    assert {:error, "missing required option: --reason"} =
             RebuildTask.parse_args(["plan", "Example.Asset", "--reason", "   "])

    assert {:error, "--plan-hash must be 64 lowercase hexadecimal characters"} =
             RebuildTask.parse_args(["start", "rebuild_plan_1", "--plan-hash", "ABC"])

    assert {:error, "invalid option for mix favn.rebuild retry"} =
             RebuildTask.parse_args(["retry", "rebuild_1", "--reason", "invalid"])
  end

  test "mix favn.rebuild prints the immutable plan details required for approval" do
    output =
      capture_io(fn ->
        RebuildTask.print_plan(%{
          "plan_id" => "rebuild_plan_1",
          "plan_hash" => String.duplicate("a", 64),
          "expires_at" => "2026-07-22T15:00:00Z",
          "payload" => %{
            "evaluated_at" => "2026-07-22T14:00:00Z",
            "root_target_id" => "asset:orders",
            "manifest_version_id" => "manifest_1",
            "required_runner_release_id" => "release_1",
            "deployment_id" => "deployment_1",
            "coverage" => %{
              "declared_from" => %{
                "kind" => "month",
                "start_at" => "2026-01-01T00:00:00Z",
                "end_at" => "2026-02-01T00:00:00Z",
                "timezone" => "Etc/UTC"
              },
              "effective_from" => %{
                "kind" => "month",
                "start_at" => "2026-03-01T00:00:00Z",
                "end_at" => "2026-04-01T00:00:00Z",
                "timezone" => "Etc/UTC"
              },
              "through" => "latest_closed",
              "timezone" => "Etc/UTC",
              "availability_delay_seconds" => 21_600
            },
            "evaluated_range" => %{
              "start_at" => "2026-03-01T00:00:00Z",
              "end_at" => "2026-07-01T00:00:00Z"
            },
            "active_generation_id" => "generation_old",
            "candidate_generation_id" => "generation_new",
            "binding_snapshot" => %{
              "asset:orders" => %{
                "compatibility_status" => "rebuild_required",
                "reason_code" => "schema_changed",
                "compatibility_diff" => %{"added_columns" => ["country"]}
              }
            },
            "capabilities" => %{
              "asset:orders" => %{"atomic_generation_activation" => true}
            },
            "actions" => [
              %{
                "ordinal" => 0,
                "target_id" => "asset:orders",
                "action" => "rebuild",
                "reason" => %{"reason_code" => "schema_changed"},
                "mapping_proof" => %{"kind" => "identity"},
                "pinned_input_generation_ids" => [%{"generation_id" => "generation_input"}],
                "candidate_generation" => %{"target_generation_id" => "generation_new"}
              }
            ],
            "item_count" => 4,
            "items_digest" => String.duplicate("b", 64)
          }
        })
      end)

    assert output =~
             "Declared coverage: month · 2026-01-01T00:00:00Z..2026-02-01T00:00:00Z · Etc/UTC"

    assert output =~
             "Effective coverage: month · 2026-03-01T00:00:00Z..2026-04-01T00:00:00Z · Etc/UTC"

    assert output =~ "Compatibility: rebuild_required"
    assert output =~ "asset:orders — rebuild"
    assert output =~ "pinned inputs:"
    assert output =~ "candidate generation: generation_new"
    assert output =~ "mix favn.rebuild start rebuild_plan_1 --plan-hash"
    refute output =~ "activation_token"
  end

  test "mix favn.rebuild reports stopped local stack", %{root_dir: root_dir} do
    assert_raise Mix.Error, ~r/stack not running; use mix favn.dev/, fn ->
      RebuildTask.run([
        "plan",
        "Example.Asset",
        "--reason",
        "schema changed",
        "--root-dir",
        root_dir
      ])
    end
  end

  test "mix favn.init requires duckdb sample flags" do
    assert_raise Mix.Error, ~r/missing required option\(s\): --duckdb, --sample/, fn ->
      InitTask.run([])
    end
  end

  test "mix favn.init rejects root-dir because it targets the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/mix favn.init --duckdb --sample/, fn ->
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

  test "mix favn.init prints the generated Compose deployment summary", %{root_dir: root_dir} do
    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule PublicTaskCompose.MixProject do
        use Mix.Project

        def project do
          [app: :public_task_compose, version: "0.1.0"]
        end
      end
      """
    )

    output =
      File.cd!(root_dir, fn ->
        capture_io(fn ->
          InitTask.run(["--target", "compose"])
        end)
      end)

    assert output =~ "Favn Compose template ready"
    assert output =~ "profile: local"
    assert output =~ "deployment: deploy/compose.local.yml"
    assert output =~ "environment reference: deploy/compose.local.env.example"
    assert File.regular?(Path.join(root_dir, "deploy/compose.local.yml"))
  end

  test "mix favn.doctor rejects root-dir because it checks the current Mix project", %{
    root_dir: root_dir
  } do
    assert_raise Mix.Error, ~r/usage: mix favn.doctor/, fn ->
      DoctorTask.run(["--root-dir", root_dir, "--skip-compile"])
    end
  end

  test "mix favn.logs accepts a run id for event output" do
    assert {:ok, {:run_events, "run_1", opts}} = LogsTask.parse_args(["run_1", "--tail", "10"])
    assert Keyword.fetch!(opts, :tail) == 10

    assert {:error, message} = LogsTask.parse_args(["run_1", "--follow"])
    assert message =~ "RUN_ID cannot be combined"
  end

  test "mix favn.logs exposes only current Compose service names" do
    for service <- ~w(postgres runner control-plane all) do
      assert {:ok, {:services, opts}} = LogsTask.parse_args(["--service", service])
      assert Keyword.fetch!(opts, :service) == service
    end

    for removed_service <- ~w(operator web orchestrator) do
      assert_raise Mix.Error, ~r/expected postgres\|control-plane\|runner\|all/, fn ->
        LogsTask.run(["--service", removed_service])
      end
    end
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

  test "mix favn.query starts the SQL runtime before connecting", %{root_dir: root_dir} do
    configure_query_connection!()
    stop_sql_runtime!()
    :ok = EnvBootstrap.install_for_current_process(:query, root_dir: root_dir)

    output =
      capture_io(fn ->
        QueryTask.run_configured([
          "select 1",
          "--connection",
          "query_test",
          "--root-dir",
          root_dir
        ])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "value"
    assert output =~ "select 1"
  end

  test "mix favn.inspect relation starts the SQL runtime before connecting", %{
    root_dir: root_dir
  } do
    configure_query_connection!()
    stop_sql_runtime!()
    :ok = EnvBootstrap.install_for_current_process(:inspect, root_dir: root_dir)

    output =
      capture_io(fn ->
        InspectTask.run_configured([
          "relation",
          "raw.sales.orders",
          "--connection",
          "query_test",
          "--root-dir",
          root_dir
        ])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "relation: query_test.raw.sales.orders"
    assert output =~ "row_count: 1"
    assert output =~ "id\tinteger"
  end

  test "mix favn.inspect partitions starts the SQL runtime before connecting", %{
    root_dir: root_dir
  } do
    configure_query_connection!()
    stop_sql_runtime!()
    :ok = EnvBootstrap.install_for_current_process(:inspect, root_dir: root_dir)

    output =
      capture_io(fn ->
        InspectTask.run_configured([
          "partitions",
          "raw.sales.orders",
          "--connection",
          "query_test",
          "--root-dir",
          root_dir
        ])
      end)

    assert Process.whereis(Favn.SQL.SessionPool)
    assert output =~ "relation: query_test.raw.sales.orders"
    assert output =~ "partitions: [%{month: \"2026-05\"}]"
  end

  test "mix favn.reset removes generated state but preserves .favn/data", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    data_file = Path.join(root_dir, ".favn/data/local.db")
    generated_file = Path.join(root_dir, ".favn/logs/generated.txt")
    File.mkdir_p!(Path.dirname(data_file))
    File.write!(data_file, "consumer data")
    File.write!(generated_file, "generated state")

    output =
      capture_io(fn ->
        ResetTask.run(["--root-dir", root_dir, "--yes"])
      end)

    assert output =~ "Favn local state reset complete"
    assert File.read!(data_file) == "consumer data"
    refute File.exists?(generated_file)
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
end
