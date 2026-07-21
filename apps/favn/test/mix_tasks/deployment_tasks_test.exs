defmodule Mix.Tasks.Favn.DeploymentTasksTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Favn.Activate
  alias Mix.Tasks.Favn.Build.Manifest
  alias Mix.Tasks.Favn.Build.ControlPlane
  alias Mix.Tasks.Favn.Publish

  setup do
    previous_url = System.get_env("FAVN_ORCHESTRATOR_URL")

    on_exit(fn ->
      if previous_url,
        do: System.put_env("FAVN_ORCHESTRATOR_URL", previous_url),
        else: System.delete_env("FAVN_ORCHESTRATOR_URL")
    end)

    :ok
  end

  test "build.manifest requires an explicit runner descriptor" do
    assert_raise Mix.Error, ~r/--runner-release/, fn -> Manifest.parse_args([]) end

    assert Manifest.parse_args(["--runner-release", "runner-release.json"]) ==
             [runner_release: "runner-release.json"]
  end

  test "maintainer control-plane build exposes only the candidate load switch" do
    assert ControlPlane.parse_args([]) == []
    assert ControlPlane.parse_args(["--load"]) == [load: true]

    assert_raise Mix.Error, ~r/invalid option/, fn ->
      ControlPlane.parse_args(["--image", "untrusted:latest"])
    end

    assert_raise Mix.Error, ~r/invalid option/, fn ->
      ControlPlane.parse_args(["--root-dir", "/tmp/other"])
    end
  end

  test "publish and activate use the canonical orchestrator URL environment" do
    System.put_env("FAVN_ORCHESTRATOR_URL", "http://control.internal")

    assert Publish.parse_args(["--manifest", "manifest-index.json"]) == [
             orchestrator_url: "http://control.internal",
             manifest_path: "manifest-index.json"
           ]

    activate =
      Activate.parse_args([
        "--manifest-version",
        "mv_exact",
        "--workspace-id",
        "workspace-a"
      ])

    assert activate[:orchestrator_url] == "http://control.internal"
    assert activate[:workspace_id] == "workspace-a"
    assert activate[:manifest_version_id] == "mv_exact"
  end

  test "service tokens are rejected as command-line arguments" do
    publish_error =
      assert_raise Mix.Error, ~r/invalid option/, fn ->
        Publish.parse_args([
          "--manifest",
          "manifest-index.json",
          "--orchestrator-url",
          "http://control.internal",
          "--service-token",
          "must-not-be-on-command-line"
        ])
      end

    activate_error =
      assert_raise Mix.Error, ~r/invalid option/, fn ->
        Activate.parse_args([
          "--manifest-version",
          "mv_exact",
          "--workspace-id",
          "workspace-a",
          "--orchestrator-url",
          "http://control.internal",
          "--service-token",
          "must-not-be-on-command-line"
        ])
      end

    refute publish_error.message =~ "must-not-be-on-command-line"
    refute activate_error.message =~ "must-not-be-on-command-line"
  end
end
