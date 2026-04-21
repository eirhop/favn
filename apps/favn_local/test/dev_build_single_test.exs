defmodule Favn.Dev.Build.SingleTest do
  use ExUnit.Case, async: true

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
             Dev.install(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

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
    assert File.exists?(Path.join(result.dist_dir, "env/web.env"))
    assert File.exists?(Path.join(result.dist_dir, "env/orchestrator.env"))
    assert File.exists?(Path.join(result.dist_dir, "env/runner.env"))
    assert File.exists?(Path.join(result.dist_dir, "bin/start"))
    assert File.exists?(Path.join(result.dist_dir, "bin/stop"))
    assert File.exists?(Path.join(result.dist_dir, "OPERATOR_NOTES.md"))

    assert {:ok, assembly_json} = File.read(Path.join(result.dist_dir, "config/assembly.json"))
    assert {:ok, metadata_json} = File.read(Path.join(result.dist_dir, "metadata.json"))
    assert {:ok, start_script} = File.read(Path.join(result.dist_dir, "bin/start"))
    assert {:ok, stop_script} = File.read(Path.join(result.dist_dir, "bin/stop"))

    assert {:ok, %{"storage" => %{"mode" => "sqlite"}}} = JSON.decode(assembly_json)

    assert {:ok,
            %{
              "artifact" => %{"kind" => "assembly_bundle", "operational" => false},
              "topology" => %{"boundary" => "web+orchestrator+runner", "collapsed" => false}
            }} = JSON.decode(metadata_json)

    assert start_script =~ "assembly-only"
    assert start_script =~ "exit 1"
    assert stop_script =~ "exit 1"
  end

  test "build_single/1 supports postgres storage override", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Dev.install(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    assert {:ok, result} =
             Dev.build_single(
               root_dir: root_dir,
               storage: :postgres,
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )

    assert {:ok, assembly_json} = File.read(Path.join(result.dist_dir, "config/assembly.json"))
    assert {:ok, %{"storage" => %{"mode" => "postgres"}}} = JSON.decode(assembly_json)
  end

  test "build_single/1 requires install", %{root_dir: root_dir} do
    assert {:error, :install_required} =
             Dev.build_single(root_dir: root_dir, skip_compile: true, skip_tool_checks: true)
  end
end
