defmodule Favn.Dev.Build.OrchestratorTest do
  use ExUnit.Case, async: true

  alias Favn.Dev

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_build_orchestrator_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root_dir, "web/favn_web"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))

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

  test "build_orchestrator/1 writes build and dist contracts", %{root_dir: root_dir} do
    assert :ok = Dev.install(root_dir: root_dir, skip_web_install: true, skip_tool_checks: true)

    assert {:ok, result} = Dev.build_orchestrator(root_dir: root_dir, skip_tool_checks: true)

    assert {:ok, build_json} = File.read(Path.join(result.build_dir, "build.json"))
    assert {:ok, metadata_json} = File.read(Path.join(result.dist_dir, "metadata.json"))
    assert File.exists?(Path.join(result.dist_dir, "bundle.json"))

    assert {:ok, %{"target" => "orchestrator", "build_id" => build_id}} = JSON.decode(build_json)

    assert {:ok, %{"target" => "orchestrator", "build_id" => ^build_id}} =
             JSON.decode(metadata_json)
  end

  test "build_orchestrator/1 requires install", %{root_dir: root_dir} do
    assert {:error, :install_required} = Dev.build_orchestrator(root_dir: root_dir)
  end
end
