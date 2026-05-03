defmodule Favn.Dev.Build.SingleTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Favn.Dev

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_build_single_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root_dir, "web/favn_web"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_duckdb"))

    File.write!(Path.join(root_dir, "mix.lock"), "lock")
    File.write!(Path.join(root_dir, "web/favn_web/package.json"), "{}")
    File.write!(Path.join(root_dir, "web/favn_web/package-lock.json"), "{}")

    File.write!(
      Path.join(root_dir, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(root_dir, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "build_single/1 writes assembled single-node bundle with sqlite default", %{
    root_dir: root_dir
  } do
    assert {:ok, :installed} =
             Dev.install(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    assert {:ok, result} =
             Dev.build_single(
               root_dir: root_dir,
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )

    assert File.exists?(Path.join(result.build_dir, "build.json"))
    assert File.exists?(Path.join(result.dist_dir, "metadata.json"))
    assert File.exists?(Path.join(result.dist_dir, "config/assembly.json"))
    assert File.exists?(Path.join(result.dist_dir, "env/backend.env.example"))
    refute File.exists?(Path.join(result.dist_dir, "env/backend.env"))
    assert executable?(Path.join(result.dist_dir, "bin/start"))
    assert executable?(Path.join(result.dist_dir, "bin/stop"))
    assert File.exists?(Path.join(result.dist_dir, "OPERATOR_NOTES.md"))

    assert {:ok, assembly_json} = File.read(Path.join(result.dist_dir, "config/assembly.json"))
    assert {:ok, metadata_json} = File.read(Path.join(result.dist_dir, "metadata.json"))
    assert {:ok, start_script} = File.read(Path.join(result.dist_dir, "bin/start"))
    assert {:ok, stop_script} = File.read(Path.join(result.dist_dir, "bin/stop"))
    assert {:ok, env_example} = File.read(Path.join(result.dist_dir, "env/backend.env.example"))

    assert {:ok, %{"storage" => %{"mode" => "sqlite"}, "services" => services}} =
             JSON.decode(assembly_json)

    assert Map.has_key?(services, "orchestrator")
    assert Map.has_key?(services, "runner")
    assert map_size(services) == 2

    assert {:ok, metadata} = JSON.decode(metadata_json)

    assert %{
             "artifact" => %{
               "kind" => "project_local_backend_launcher",
                "operational" => true,
                "relocatable" => false
             },
             "compatibility" => %{
               "storage_modes" => ["sqlite"],
               "runtime_dependency" => "recorded_orchestrator_source_root",
               "unsupported" => unsupported
             },
             "required_env" => required_env,
             "topology" => %{
               "backend_only" => true,
               "boundary" => "orchestrator+runner+scheduler",
               "boundary_preserved" => true,
               "scheduler_instances" => 1
             }
           } = metadata

    assert "postgres_production_mode" in unsupported
    assert "self_contained_release_artifact" in unsupported
    refute "postgres" in get_in(metadata, ["compatibility", "storage_modes"])
    assert "FAVN_STORAGE" in required_env
    assert "FAVN_SQLITE_PATH" in required_env
    assert "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" in required_env
    assert "FAVN_RUNNER_MODE" in required_env
    refute Enum.any?(required_env, &String.starts_with?(&1, "FAVN_DEV_"))

    assert start_script =~ "Application.ensure_all_started(:favn_runner)"
    assert start_script =~ "Application.ensure_all_started(:favn_storage_sqlite)"
    assert start_script =~ "Application.ensure_all_started(:favn_orchestrator)"
    assert start_script =~ "/api/orchestrator/v1/health/ready"
    assert start_script =~ "cat > \"$BOOT_FILE\" <<'EOF'\nartifact_root"
    assert start_script =~ "\nEOF\n\n("
    assert start_script =~ "FAVN_SCHEDULER_ENABLED"
    assert start_script =~ "env/backend.env"
    refute start_script =~ "assembly-only"
    refute start_script =~ "No operational runtime launch wiring"
    refute stop_script =~ "No managed processes were started"
    refute env_example =~ "FAVN_DEV_"
    assert env_example =~
             "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS=favn_web:replace-with-32-plus-char-service-token"
    refute start_script =~ "FAVN_DEV_"
    refute stop_script =~ "FAVN_DEV_"
    refute metadata_json =~ "FAVN_DEV_"
  end

  test "build_single/1 rejects postgres storage override", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Dev.install(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    assert {:error, {:unsupported_storage, :postgres}} =
             Dev.build_single(
               root_dir: root_dir,
               storage: :postgres,
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )

    assert {:error, {:unsupported_storage, :postgres}} =
             Dev.build_single(
               root_dir: root_dir,
               storage: "postgres",
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )
  end

  test "build_single/1 rejects unsupported string storage without atomizing it", %{
    root_dir: root_dir
  } do
    assert {:error, {:invalid_storage, "bogus"}} =
             Dev.build_single(
               root_dir: root_dir,
               storage: "bogus",
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )
  end

  test "build_single/1 requires install", %{root_dir: root_dir} do
    assert {:error, :install_required} =
             Dev.build_single(root_dir: root_dir, skip_compile: true, skip_tool_checks: true)
  end

  defp executable?(path) do
    case File.stat(path) do
      {:ok, %{mode: mode}} -> (mode &&& 0o111) != 0
      _other -> false
    end
  end
end
