defmodule Favn.Dev.Build.RunnerTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
  alias Favn.Dev.Paths

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_build_runner_test_#{System.unique_integer([:positive])}"
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

  test "build_runner/1 writes build and dist contracts", %{root_dir: root_dir} do
    assert {:ok, :installed} =
             Dev.install(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    assert {:ok, result} =
             Dev.build_runner(
               root_dir: root_dir,
               skip_compile: true,
               skip_tool_checks: true,
               skip_project_root_check: true
             )

    build_json_path = Path.join(result.build_dir, "build.json")
    metadata_json_path = Path.join(result.dist_dir, "metadata.json")
    manifest_json_path = Path.join(result.dist_dir, "manifest.json")

    assert File.exists?(build_json_path)
    assert File.exists?(metadata_json_path)
    assert File.exists?(manifest_json_path)
    assert File.exists?(Path.join(result.dist_dir, "OPERATOR_NOTES.md"))

    assert {:ok, build_json} = File.read(build_json_path)
    assert {:ok, metadata_json} = File.read(metadata_json_path)

    assert {:ok, %{"target" => "runner", "build_id" => build_id}} = JSON.decode(build_json)

    assert {:ok,
            %{
              "target" => "runner",
              "build_id" => ^build_id,
              "artifact" => %{"kind" => "runtime_package", "operational" => true},
              "compatibility" => compatibility
            }} =
             JSON.decode(metadata_json)

    assert is_map(compatibility)

    cache_path =
      Path.join(Paths.manifest_cache_dir(root_dir), metadata_manifest_id(metadata_json))

    assert File.exists?(cache_path)
  end

  test "build_runner/1 requires install", %{root_dir: root_dir} do
    assert {:error, :install_required} =
             Dev.build_runner(
               root_dir: root_dir,
               skip_compile: true,
               skip_project_root_check: true
             )
  end

  test "build_runner/1 rejects root_dir that is not the current project root", %{
    root_dir: root_dir
  } do
    assert {:error, {:unsupported_root_dir, _requested, _current}} =
             Dev.build_runner(root_dir: root_dir, skip_compile: true, skip_tool_checks: true)
  end

  defp metadata_manifest_id(metadata_json) do
    {:ok, %{"manifest" => %{"manifest_version_id" => id}}} = JSON.decode(metadata_json)
    "#{id}.json"
  end
end
