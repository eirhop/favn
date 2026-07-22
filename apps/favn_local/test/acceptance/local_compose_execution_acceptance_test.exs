defmodule Favn.Local.ComposeExecutionAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{
    Activate,
    ComposeDeployment,
    ComposeEnv,
    ComposeLifecycle,
    ComposeProject,
    Docker,
    Install,
    OrchestratorClient,
    Paths,
    Publish,
    Reset,
    State
  }

  alias Favn.Dev.Init.Compose, as: ComposeInit

  @moduletag :integration
  @moduletag :container
  @moduletag timeout: 1_200_000

  @pipeline_id "pipeline:Elixir.FavnIssue262Sample.Pipelines.ProductionSmoke:production_smoke"
  @elixir_asset_id "asset:Elixir.FavnIssue262Sample.Assets.SourceCheck:asset"
  @generated_pipeline_id "pipeline:Elixir.FavnGeneratedSample.Pipelines.LocalSmoke:local_smoke"
  @runner_environment %{
    "FAVN_CANONICAL_DELAY_MS" => "0",
    "FAVN_CANONICAL_DUCKDB_PATH" => "/var/lib/favn/data/canonical-acceptance.duckdb",
    "FAVN_CANONICAL_MISSING_SECRET" => "compose-acceptance-present-secret",
    "FAVN_CANONICAL_SOURCE_NAME" => "compose-acceptance",
    "FAVN_CANONICAL_SOURCE_TOKEN" => "compose-acceptance-source-token",
    "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS" => "5000"
  }

  setup do
    candidate =
      System.get_env("FAVN_CONTROL_PLANE_CANDIDATE") ||
        raise "FAVN_CONTROL_PLANE_CANDIDATE must name the repository-built candidate image"

    {:ok, image} = Docker.inspect_image(candidate)
    root_dir = Favn.Local.CanonicalSampleProject.create!("favn_local_compose_execution")
    scaffold_generated_sample!(root_dir)
    run_mix!(root_dir, ["deps.get"])
    assert {:ok, _scaffold} = ComposeInit.run(root_dir: root_dir)

    opts = [
      root_dir: root_dir,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      candidate_control_plane: %{"reference" => candidate, "image_id" => image.id},
      web_port: free_port(),
      orchestrator_port: free_port(),
      progress_fun: fn _message -> :ok end,
      ready_timeout_ms: 180_000,
      docker_build_timeout_ms: 1_200_000,
      compose_command_timeout_ms: 600_000,
      runner_build_fun: &build_consumer_runner/1,
      env_file_loaded: @runner_environment,
      foreground: false
    ]

    assert {:ok, :installed} = Install.run(opts)
    project_name = ComposeProject.project_name(root_dir)

    on_exit(fn ->
      _ = ComposeLifecycle.stop(opts)
      _ = Reset.run(Keyword.put(opts, :yes, true))
      cleanup_project_resources(project_name)
      File.rm_rf(root_dir)
    end)

    %{opts: opts, root_dir: root_dir}
  end

  test "canonical customer runner executes Elixir and SQL assets", context do
    assert {:ok, started} = ComposeLifecycle.start(context.opts)
    deployment = deployment!(context.opts)
    assert {:ok, secrets} = State.read_secrets(root_dir: context.root_dir)

    assert {:ok, session} =
             OrchestratorClient.password_login(
               started.orchestrator_url,
               secrets["service_token"],
               "local-dev",
               "admin",
               secrets["bootstrap_password"]
             )

    assert {:ok, %{"manifest" => manifest}} =
             OrchestratorClient.bootstrap_active_manifest(
               started.orchestrator_url,
               secrets["service_token"],
               session
             )

    payload = %{
      target: %{type: "pipeline", id: @pipeline_id},
      manifest_selection: %{
        mode: "version",
        manifest_version_id: manifest["manifest_version_id"]
      }
    }

    assert {:ok, run} =
             OrchestratorClient.submit_run(
               started.orchestrator_url,
               secrets["service_token"],
               session,
               payload
             )

    terminal =
      await_terminal_run!(started.orchestrator_url, secrets, session, run["id"], context.opts)

    assert terminal["status"] == "ok", compose_logs(context.opts)

    outcomes =
      terminal["asset_results"]
      |> Enum.map(&{&1["asset_ref"], &1["status"]})
      |> Map.new()

    assert outcomes["Elixir.FavnIssue262Sample.Assets.SourceCheck:asset"] == "ok"
    assert outcomes["Elixir.FavnIssue262Sample.Lakehouse.Raw.Sales.Orders:asset"] == "ok"

    assert outcomes[
             "Elixir.FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary:asset"
           ] == "ok"

    summary =
      Enum.find(terminal["asset_results"], fn result ->
        result["asset_ref"] ==
          "Elixir.FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary:asset"
      end)

    assert get_in(summary, ["output_metadata", "materialized", "schema"]) == "mart"
    assert get_in(summary, ["output_metadata", "materialized", "name"]) == "order_summary"

    generated_payload =
      put_in(payload, [:target], %{type: "pipeline", id: @generated_pipeline_id})

    assert {:ok, generated_run} =
             OrchestratorClient.submit_run(
               started.orchestrator_url,
               secrets["service_token"],
               session,
               generated_payload
             )

    generated_terminal =
      await_terminal_run!(
        started.orchestrator_url,
        secrets,
        session,
        generated_run["id"],
        context.opts
      )

    assert generated_terminal["status"] == "ok", compose_logs(context.opts)

    generated_outcomes =
      generated_terminal["asset_results"]
      |> Enum.map(&{&1["asset_ref"], &1["status"]})
      |> Map.new()

    assert generated_outcomes[
             "Elixir.FavnGeneratedSample.Lakehouse.Raw.Sales.Orders:asset"
           ] == "ok"

    assert generated_outcomes[
             "Elixir.FavnGeneratedSample.Lakehouse.Mart.Sales.OrderSummary:asset"
           ] == "ok"

    sample_paths = generated_sample_paths(context.root_dir)
    assert Enum.all?(sample_paths, &File.regular?/1)
    sample_sizes = Map.new(sample_paths, &{&1, File.stat!(&1).size})

    assert :ok = ComposeLifecycle.stop(context.opts)
    assert Enum.all?(sample_sizes, fn {path, size} -> File.stat!(path).size == size end)
    assert {:ok, restarted} = ComposeLifecycle.start(context.opts)
    assert Enum.all?(sample_sizes, fn {path, size} -> File.stat!(path).size == size end)

    assert {:ok, restarted_session} =
             OrchestratorClient.password_login(
               restarted.orchestrator_url,
               secrets["service_token"],
               "local-dev",
               "admin",
               secrets["bootstrap_password"]
             )

    assert {:ok, %{"status" => "ok"}} =
             OrchestratorClient.get_run(
               restarted.orchestrator_url,
               secrets["service_token"],
               restarted_session,
               run["id"]
             )

    assert {:ok, %{"manifest" => restarted_manifest}} =
             OrchestratorClient.bootstrap_active_manifest(
               restarted.orchestrator_url,
               secrets["service_token"],
               restarted_session
             )

    restarted_payload =
      put_in(
        payload,
        [:manifest_selection, :manifest_version_id],
        restarted_manifest["manifest_version_id"]
      )

    assert {:ok, restarted_run} =
             OrchestratorClient.submit_run(
               restarted.orchestrator_url,
               secrets["service_token"],
               restarted_session,
               restarted_payload
             )

    restarted_terminal =
      await_terminal_run!(
        restarted.orchestrator_url,
        secrets,
        restarted_session,
        restarted_run["id"],
        context.opts
      )

    assert restarted_terminal["status"] == "ok", compose_logs(context.opts)

    assert_control_plane_upgrade_and_rollback!(
      deployment,
      context.opts,
      secrets,
      payload
    )

    before_sql_only = inspect_services(deployment)
    assert {:ok, before_sql_latest} = State.read_runner_latest(root_dir: context.root_dir)

    replace_source!(
      context.root_dir,
      "lib/favn_issue262_sample/lakehouse/mart/sales/order_summary.ex",
      "category: :orders,",
      "category: :orders_sql_only_update,"
    )

    assert :ok = ComposeLifecycle.reload(context.opts)
    after_sql_only = inspect_services(deployment)
    assert {:ok, sql_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert sql_latest["runner_release_id"] == before_sql_latest["runner_release_id"]
    assert sql_latest["image_id"] == before_sql_latest["image_id"]
    refute sql_latest["manifest_version_id"] == before_sql_latest["manifest_version_id"]

    for service <- ["runner", "control-plane"] do
      assert after_sql_only[service]["Image"] == before_sql_only[service]["Image"]

      assert after_sql_only[service]["State"]["StartedAt"] ==
               before_sql_only[service]["State"]["StartedAt"]
    end

    replace_source!(
      context.root_dir,
      "lib/favn_issue262_sample/assets/source_check.ex",
      "Process.sleep(String.to_integer(ctx.runtime_config.source_system.delay_ms))",
      """
      Process.sleep(String.to_integer(ctx.runtime_config.source_system.delay_ms))
              Process.put(:favn_acceptance_runtime_marker, :changed_elixir_code)
      """
      |> String.trim()
    )

    assert_stale_descriptor_rejected!(context.root_dir, sql_latest["descriptor_path"])
    assert :ok = build_consumer_runner(context.opts)
    assert {:ok, changed_latest} = State.read_runner_latest(root_dir: context.root_dir)
    refute changed_latest["runner_release_id"] == sql_latest["runner_release_id"]

    assert {:ok, staged} =
             Publish.run(
               manifest_path: Path.join(changed_latest["manifest_dir"], "manifest-index.json"),
               orchestrator_url: restarted.orchestrator_url,
               env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"]}
             )

    assert {:error, activation_error} =
             Activate.run(
               orchestrator_url: restarted.orchestrator_url,
               manifest_version_id: staged.manifest_version_id,
               workspace_id: deployment.workspace_id,
               env: %{"FAVN_ORCHESTRATOR_SERVICE_TOKEN" => secrets["service_token"]}
             )

    assert inspect(activation_error) =~ "runner_release_mismatch"

    assert {:ok, %{"manifest" => still_active}} =
             OrchestratorClient.bootstrap_active_manifest(
               restarted.orchestrator_url,
               secrets["service_token"],
               restarted_session
             )

    assert still_active["manifest_version_id"] == sql_latest["manifest_version_id"]

    assert :ok = State.write_runner_latest(sql_latest, root_dir: context.root_dir)
    assert :ok = ComposeLifecycle.reload(context.opts)
    assert {:ok, activated_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert activated_latest["runner_release_id"] == changed_latest["runner_release_id"]

    assert_shutdown_and_dependency_recovery!(
      deployment,
      context.opts,
      secrets,
      restarted_session,
      payload
    )
  end

  defp scaffold_generated_sample!(root_dir) do
    assert {:ok, result} =
             Favn.Dev.init(
               root_dir: root_dir,
               app: :favn_issue262_sample,
               base_module: "FavnGeneratedSample",
               duckdb: true,
               sample: true
             )

    assert result.pipeline_module == "FavnGeneratedSample.Pipelines.LocalSmoke"

    File.write!(
      Path.join(root_dir, "config/config.exs"),
      """

      config :favn,
        asset_modules: :all,
        pipeline_modules: :all,
        connection_modules: :all,
        schedule_modules: :all,
        connections: [
          issue262_warehouse: [
            open: [
              database: %{
                __struct__: Favn.RuntimeConfig.Ref,
                provider: :env,
                key: "FAVN_CANONICAL_DUCKDB_PATH",
                secret?: false,
                required?: true
              }
            ],
            duckdb: []
          ],
          important_lakehouse: [
            open: [
              database: %{
                __struct__: Favn.RuntimeConfig.Ref,
                provider: :env,
                key: "FAVN_LOCAL_SAMPLE_DATABASE_PATH",
                secret?: false,
                required?: true
              }
            ],
            duckdb: [
              resources: [
                raw_catalog: [
                  file: {:priv, :favn_issue262_sample, "duckdb/raw_catalog.sql"},
                  params: [
                    database_path: %{
                      __struct__: Favn.RuntimeConfig.Ref,
                      provider: :env,
                      key: "FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH",
                      secret?: false,
                      required?: true
                    }
                  ]
                ],
                mart_catalog: [
                  file: {:priv, :favn_issue262_sample, "duckdb/mart_catalog.sql"},
                  params: [
                    database_path: %{
                      __struct__: Favn.RuntimeConfig.Ref,
                      provider: :env,
                      key: "FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH",
                      secret?: false,
                      required?: true
                    }
                  ]
                ]
              ],
              catalogs: [
                raw: [resource: :raw_catalog, write_concurrency: 1],
                mart: [resource: :mart_catalog, write_concurrency: 1]
              ]
            ]
          ]
        ]
      """,
      [:append]
    )
  end

  defp generated_sample_paths(root_dir) do
    Enum.map(
      ["local_smoke.duckdb", "raw.duckdb", "mart.duckdb"],
      &Path.join(root_dir, ".favn/data/#{&1}")
    )
  end

  defp build_consumer_runner(opts) do
    root_dir = Favn.Dev.Paths.root_dir(opts) |> Path.expand()

    expression = """
    opts = [
      root_dir: #{inspect(root_dir)},
      skip_compile: true,
      allow_non_prod_build: true,
      allow_unpinned_favn: true
    ]

    case Favn.Dev.build_runner(opts) do
      {:ok, _result} -> :ok
      {:error, reason} -> raise "consumer runner build failed: \#{inspect(reason)}"
    end
    """

    case System.cmd(
           System.find_executable("mix") || "mix",
           ["run", "--no-start", "-e", expression],
           cd: root_dir,
           env: [{"MIX_ENV", "test"}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:runner_release_build_failed, status, bounded(output)}}
    end
  end

  defp assert_stale_descriptor_rejected!(root_dir, descriptor_path) do
    expression = """
    opts = [
      root_dir: #{inspect(root_dir)},
      runner_release: #{inspect(descriptor_path)},
      allow_non_prod_build: true,
      allow_unpinned_favn: true
    ]

    case Favn.Dev.build_manifest(opts) do
      {:error, {:runner_rebuild_required, categories}} ->
        IO.puts("runner_rebuild_required=" <> inspect(categories))

      other ->
        raise "stale runner descriptor was accepted: \#{inspect(other)}"
    end
    """

    assert {output, 0} =
             System.cmd(
               System.find_executable("mix") || "mix",
               ["run", "--no-start", "-e", expression],
               cd: root_dir,
               env: [{"MIX_ENV", "test"}],
               stderr_to_stdout: true
             )

    assert output =~ "runner_rebuild_required="
    assert output =~ ":runtime_code"
  end

  defp assert_control_plane_upgrade_and_rollback!(deployment, opts, secrets, payload) do
    original_reference = deployment.control_plane_image
    upgrade_reference = acceptance_image_reference("upgrade")
    base_reference = acceptance_image_reference("base")
    context_dir = Path.join(Path.dirname(deployment.compose_file), "upgrade-context")

    File.mkdir_p!(context_dir)

    File.write!(
      Path.join(context_dir, "Dockerfile"),
      "ARG BASE_IMAGE\nFROM ${BASE_IMAGE}\nLABEL io.favn.acceptance.schema-upgrade=\"true\"\n"
    )

    on_exit(fn ->
      _ = System.cmd("docker", ["image", "rm", "--force", upgrade_reference])
      _ = System.cmd("docker", ["image", "rm", "--force", base_reference])
    end)

    assert {_output, 0} =
             System.cmd("docker", ["image", "tag", original_reference, base_reference])

    assert {_output, 0} =
             System.cmd(
               "docker",
               [
                 "build",
                 "--build-arg",
                 "BASE_IMAGE=#{base_reference}",
                 "--tag",
                 upgrade_reference,
                 context_dir
               ],
               stderr_to_stdout: true
             )

    release_operation!(deployment, "preflight-upgrade", opts)
    stop_control_plane!(deployment, opts)
    downgrade_runner_identity_schema!(deployment)
    put_compose_environment!(deployment, %{"FAVN_CONTROL_PLANE_IMAGE" => upgrade_reference})
    release_operation!(deployment, "preflight-upgrade", opts)
    release_operation!(deployment, "migrate", opts)
    release_operation!(deployment, "grant-runtime", opts)
    release_operation!(deployment, "verify-schema", opts)
    recreate_control_plane!(deployment, opts)
    await_stack_ready!(opts)
    assert_control_plane_smoke!(deployment, opts, secrets, payload)

    put_compose_environment!(deployment, %{"FAVN_CONTROL_PLANE_IMAGE" => original_reference})
    release_operation!(deployment, "preflight-upgrade", opts)
    recreate_control_plane!(deployment, opts)
    release_operation!(deployment, "verify-schema", opts)
    await_stack_ready!(opts)
    assert_control_plane_smoke!(deployment, opts, secrets, payload)
  end

  defp acceptance_image_reference(kind) do
    "favn-control-plane-acceptance-#{kind}:#{System.unique_integer([:positive])}"
  end

  defp downgrade_runner_identity_schema!(deployment) do
    assert {:ok, environment} = ComposeEnv.read(deployment.env_file)
    postgres = inspect_service(deployment, "postgres")

    sql = """
    ALTER TABLE favn_control.manifest_versions
      DROP CONSTRAINT IF EXISTS manifest_versions_runner_release_valid;
    ALTER TABLE favn_control.manifest_versions
      DROP COLUMN IF EXISTS required_runner_release_id;
    DELETE FROM favn_control.schema_migrations WHERE version = 20260721000000;
    """

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "exec",
                 postgres["Id"],
                 "psql",
                 "--username",
                 environment["FAVN_POSTGRES_ADMIN_USER"],
                 "--dbname",
                 environment["FAVN_POSTGRES_DATABASE"],
                 "--set",
                 "ON_ERROR_STOP=1",
                 "--command",
                 sql
               ],
               stderr_to_stdout: true
             )

    assert output =~ "DELETE 1"
  end

  defp release_operation!(deployment, operation, opts) do
    service =
      if operation == "verify-schema", do: "control-plane-verify", else: "control-plane-ops"

    assert {output, 0} =
             Docker.compose(
               deployment,
               ["--profile", "operations", "run", "--rm", service, operation],
               Keyword.put(opts, :compose_command_timeout_ms, 600_000)
             )

    assert output =~ "status=ok"
  end

  defp stop_control_plane!(deployment, opts) do
    assert {_output, 0} =
             Docker.compose(
               deployment,
               ["stop", "--timeout", "180", "control-plane"],
               Keyword.put(opts, :compose_command_timeout_ms, 300_000)
             )
  end

  defp recreate_control_plane!(deployment, opts) do
    assert {_output, 0} =
             Docker.compose(
               deployment,
               ["up", "--detach", "--wait", "--no-deps", "--force-recreate", "control-plane"],
               Keyword.put(opts, :compose_command_timeout_ms, 300_000)
             )
  end

  defp assert_control_plane_smoke!(deployment, opts, secrets, payload) do
    assert {:ok, session} =
             OrchestratorClient.password_login(
               deployment.orchestrator_url,
               secrets["service_token"],
               "local-dev",
               "admin",
               secrets["bootstrap_password"]
             )

    run_id = submit_active_pipeline!(deployment, secrets, session, payload)

    assert await_terminal_run!(deployment.orchestrator_url, secrets, session, run_id, opts)[
             "status"
           ] == "ok"
  end

  defp assert_shutdown_and_dependency_recovery!(deployment, opts, secrets, session, payload) do
    asset_payload = put_in(payload, [:target], %{type: "asset", id: @elixir_asset_id})

    configure_runner!(deployment, opts, %{
      "FAVN_CANONICAL_DELAY_MS" => "1500",
      "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS" => "5000"
    })

    control_plane_run = submit_active_pipeline!(deployment, secrets, session, payload)

    assert %{"status" => "running"} =
             await_run_status!(deployment, secrets, session, control_plane_run)

    await_runner_active!(deployment)
    control_plane_before = inspect_service(deployment, "control-plane")

    signal_and_restart!(
      deployment,
      "control-plane",
      control_plane_before["Id"],
      control_plane_before["State"]["StartedAt"]
    )

    control_plane_terminal =
      await_terminal_run!(
        deployment.orchestrator_url,
        secrets,
        session,
        control_plane_run,
        opts
      )

    assert control_plane_terminal["status"] in ["ok", "error", "cancelled", "timed_out"]

    if control_plane_terminal["status"] == "error" do
      assert is_map(control_plane_terminal["error"])
    end

    await_stack_ready!(opts)

    runner_run = submit_active_pipeline!(deployment, secrets, session, asset_payload)
    assert %{"status" => "running"} = await_run_status!(deployment, secrets, session, runner_run)
    await_runner_active!(deployment)
    runner_before = inspect_service(deployment, "runner")

    signal_and_restart!(
      deployment,
      "runner",
      runner_before["Id"],
      runner_before["State"]["StartedAt"]
    )

    assert await_terminal_run!(deployment.orchestrator_url, secrets, session, runner_run, opts)[
             "status"
           ] == "ok"

    await_stack_ready!(opts)

    configure_runner!(deployment, opts, %{
      "FAVN_CANONICAL_DELAY_MS" => "3500",
      "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS" => "1000"
    })

    deadline_run = submit_active_pipeline!(deployment, secrets, session, asset_payload)

    assert %{"status" => "running"} =
             await_run_status!(deployment, secrets, session, deadline_run)

    await_runner_active!(deployment)
    deadline_runner = inspect_service(deployment, "runner")

    signal_and_restart!(
      deployment,
      "runner",
      deadline_runner["Id"],
      deadline_runner["State"]["StartedAt"]
    )

    assert %{"status" => deadline_status} =
             await_terminal_run!(
               deployment.orchestrator_url,
               secrets,
               session,
               deadline_run,
               opts
             )

    assert deadline_status in ["error", "cancelled", "timed_out"]
    await_stack_ready!(opts)

    configure_runner!(deployment, opts, %{
      "FAVN_CANONICAL_DELAY_MS" => "1500",
      "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS" => "5000"
    })

    completion_run = submit_active_pipeline!(deployment, secrets, session, asset_payload)

    assert %{"status" => "running"} =
             await_run_status!(deployment, secrets, session, completion_run)

    await_runner_active!(deployment)
    control_plane = inspect_service(deployment, "control-plane")
    stop_container_abruptly!(control_plane["Id"])
    Process.sleep(2_000)
    start_container!(control_plane["Id"])
    await_service_restart!(deployment, "control-plane", control_plane["State"]["StartedAt"])

    completion_terminal =
      await_terminal_run!(
        deployment.orchestrator_url,
        secrets,
        session,
        completion_run,
        opts
      )

    assert completion_terminal["status"] == "error"
    assert is_map(completion_terminal["error"])

    await_stack_ready!(opts)

    dependency_run = submit_active_pipeline!(deployment, secrets, session, asset_payload)

    assert %{"status" => "running"} =
             await_run_status!(deployment, secrets, session, dependency_run)

    await_runner_active!(deployment)
    runner = inspect_service(deployment, "runner")
    stop_container_abruptly!(runner["Id"])

    assert %{"status" => dependency_status} =
             await_terminal_run!(
               deployment.orchestrator_url,
               secrets,
               session,
               dependency_run,
               opts
             )

    assert dependency_status in ["error", "cancelled", "timed_out"]
    start_container!(runner["Id"])
    await_service_restart!(deployment, "runner", runner["State"]["StartedAt"])
    await_stack_ready!(opts)

    idle_control_plane = inspect_service(deployment, "control-plane")

    signal_and_restart!(
      deployment,
      "control-plane",
      idle_control_plane["Id"],
      idle_control_plane["State"]["StartedAt"]
    )

    await_stack_ready!(opts)
  end

  defp configure_runner!(deployment, opts, updates) do
    environment = Map.merge(@runner_environment, updates)

    assert :ok =
             ComposeProject.put_runner_environment(
               %{"runner_env_path" => Paths.compose_runner_env_path(deployment.root_dir)},
               environment
             )

    assert {_output, 0} =
             Docker.compose(
               deployment,
               ["up", "--detach", "--wait", "--no-deps", "--force-recreate", "runner"],
               Keyword.put(opts, :compose_command_timeout_ms, 300_000)
             )

    await_stack_ready!(opts)
  end

  defp put_compose_environment!(deployment, updates) do
    assert {:ok, environment} = ComposeEnv.read(deployment.env_file)
    assert {:ok, encoded} = ComposeEnv.encode(Map.merge(environment, updates))
    assert :ok = File.write(deployment.env_file, encoded)
    assert :ok = File.chmod(deployment.env_file, 0o600)
  end

  defp submit_active_pipeline!(deployment, secrets, session, payload) do
    assert {:ok, %{"manifest" => active}} =
             OrchestratorClient.bootstrap_active_manifest(
               deployment.orchestrator_url,
               secrets["service_token"],
               session
             )

    active_payload =
      put_in(
        payload,
        [:manifest_selection, :manifest_version_id],
        active["manifest_version_id"]
      )

    assert {:ok, run} =
             OrchestratorClient.submit_run(
               deployment.orchestrator_url,
               secrets["service_token"],
               session,
               active_payload
             )

    run["id"]
  end

  defp await_run_status!(deployment, secrets, session, run_id, attempts \\ 120)

  defp await_run_status!(deployment, secrets, session, run_id, attempts) when attempts > 0 do
    case OrchestratorClient.get_run(
           deployment.orchestrator_url,
           secrets["service_token"],
           session,
           run_id
         ) do
      {:ok, %{"status" => "running"} = run} ->
        run

      {:ok, %{"status" => status} = run}
      when status in ["ok", "error", "cancelled", "timed_out"] ->
        flunk("run #{run_id} became #{status} before the disruption: #{inspect(run)}")

      _pending_or_unavailable ->
        Process.sleep(100)
        await_run_status!(deployment, secrets, session, run_id, attempts - 1)
    end
  end

  defp await_run_status!(_project, _secrets, _session, run_id, 0),
    do: flunk("run #{run_id} never reached running")

  defp await_runner_active!(deployment, attempts \\ 120)

  defp await_runner_active!(deployment, attempts) when attempts > 0 do
    runner = inspect_service(deployment, "runner")

    expression =
      "case FavnRunner.diagnostics() do {:ok, value} -> IO.puts(value.admission.active_worker_count); other -> raise inspect(other) end"

    case System.cmd(
           "docker",
           ["exec", runner["Id"], "/opt/favn/bin/favn_runner", "rpc", expression],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        if Regex.match?(~r/(?:^|\n)[1-9][0-9]*(?:\n|$)/, output) do
          :ok
        else
          Process.sleep(100)
          await_runner_active!(deployment, attempts - 1)
        end

      {_output, _status} ->
        Process.sleep(100)
        await_runner_active!(deployment, attempts - 1)
    end
  end

  defp await_runner_active!(_project, 0), do: flunk("runner worker never became active")

  defp signal_container!(container_id, signal) do
    assert {_output, 0} = System.cmd("docker", ["kill", "--signal", signal, container_id])
  end

  defp signal_and_restart!(deployment, service, container_id, previous_started_at) do
    signal_container!(container_id, "TERM")
    await_container_stopped!(container_id)
    start_container!(container_id)
    await_service_restart!(deployment, service, previous_started_at)
  end

  defp await_container_stopped!(container_id, attempts \\ 240)

  defp await_container_stopped!(container_id, attempts) when attempts > 0 do
    assert {running, 0} =
             System.cmd("docker", [
               "container",
               "inspect",
               "--format",
               "{{.State.Running}}",
               container_id
             ])

    if String.trim(running) == "false" do
      :ok
    else
      Process.sleep(250)
      await_container_stopped!(container_id, attempts - 1)
    end
  end

  defp await_container_stopped!(container_id, 0),
    do: flunk("container #{container_id} did not stop after SIGTERM")

  defp stop_container_abruptly!(container_id) do
    assert {_output, 0} = System.cmd("docker", ["update", "--restart", "no", container_id])
    signal_container!(container_id, "KILL")
  end

  defp start_container!(container_id) do
    assert {_output, 0} = System.cmd("docker", ["start", container_id])

    assert {_output, 0} =
             System.cmd("docker", ["update", "--restart", "unless-stopped", container_id])
  end

  defp await_service_restart!(deployment, service, previous_started_at, attempts \\ 240)

  defp await_service_restart!(deployment, service, previous_started_at, attempts)
       when attempts > 0 do
    inspection = inspect_service(deployment, service)

    if inspection["State"]["Running"] and
         inspection["State"]["StartedAt"] != previous_started_at and
         get_in(inspection, ["State", "Health", "Status"]) == "healthy" do
      inspection
    else
      Process.sleep(250)
      await_service_restart!(deployment, service, previous_started_at, attempts - 1)
    end
  end

  defp await_service_restart!(_deployment, service, _previous_started_at, 0),
    do: flunk("#{service} did not restart healthy")

  defp await_stack_ready!(opts, attempts \\ 240)

  defp await_stack_ready!(opts, attempts) when attempts > 0 do
    case ComposeLifecycle.status(opts) do
      %{stack_status: :running, runtime: %{"status" => "ok"}} ->
        :ok

      _not_ready ->
        Process.sleep(250)
        await_stack_ready!(opts, attempts - 1)
    end
  end

  defp await_stack_ready!(_opts, 0), do: flunk("Compose stack did not recover readiness")

  defp inspect_services(deployment) do
    Map.new(["runner", "control-plane"], fn service ->
      {service, inspect_service(deployment, service)}
    end)
  end

  defp inspect_service(deployment, service) do
    assert {container, 0} =
             Docker.compose(deployment, ["ps", "--all", "--quiet", service])

    container = String.trim(container)
    assert container != ""
    assert {encoded, 0} = System.cmd("docker", ["container", "inspect", container])
    assert {:ok, [inspection]} = JSON.decode(encoded)
    inspection
  end

  defp replace_source!(root_dir, relative_path, before, replacement) do
    path = Path.join(root_dir, relative_path)
    source = File.read!(path)
    assert source =~ before
    File.write!(path, String.replace(source, before, replacement, global: false))
  end

  defp await_terminal_run!(base_url, secrets, session, run_id, opts, attempts \\ 240)

  defp await_terminal_run!(base_url, secrets, session, run_id, opts, attempts)
       when attempts > 0 do
    case OrchestratorClient.get_run(base_url, secrets["service_token"], session, run_id) do
      {:ok, %{"status" => status} = run}
      when status in ["ok", "error", "cancelled", "timed_out"] ->
        run

      {:ok, _run} ->
        Process.sleep(250)
        await_terminal_run!(base_url, secrets, session, run_id, opts, attempts - 1)

      {:error, _temporarily_unavailable} ->
        Process.sleep(250)
        await_terminal_run!(base_url, secrets, session, run_id, opts, attempts - 1)
    end
  end

  defp await_terminal_run!(_base_url, _secrets, _session, run_id, opts, 0),
    do: flunk("run #{run_id} did not finish\n#{compose_logs(opts)}")

  defp compose_logs(opts) do
    ExUnit.CaptureIO.capture_io(fn ->
      _ = ComposeLifecycle.logs(Keyword.put(opts, :tail, 300))
    end)
  end

  defp deployment!(opts) do
    assert {:ok, runtime} = State.read_runtime(opts)
    assert {:ok, deployment} = ComposeDeployment.from_runtime(runtime, opts)
    deployment
  end

  defp cleanup_project_resources(project_name) do
    {containers, 0} =
      System.cmd("docker", [
        "container",
        "ls",
        "--all",
        "--quiet",
        "--filter",
        "label=com.docker.compose.project=#{project_name}"
      ])

    case String.split(containers, "\n", trim: true) do
      [] -> :ok
      ids -> _ = System.cmd("docker", ["container", "rm", "--force" | ids])
    end

    _ = System.cmd("docker", ["network", "rm", project_name <> "-network"])
    _ = System.cmd("docker", ["volume", "rm", "--force", project_name <> "-postgres-data"])
    :ok
  end

  defp run_mix!(root_dir, args) do
    case System.cmd(System.find_executable("mix") || "mix", args,
           cd: root_dir,
           env: [{"MIX_ENV", "test"}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> flunk("mix #{Enum.join(args, " ")} failed (#{status}):\n#{output}")
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp bounded(output) when is_binary(output),
    do: output |> String.trim() |> String.slice(-8_192, 8_192)
end
