defmodule Favn.Local.UserOwnedRunnerAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Build.Manifest
  alias Favn.Dev.Init.Runner
  alias Favn.Dev.State

  @moduletag :acceptance

  test "the scaffold and manifest builder share one operator-owned release ID" do
    root_dir =
      Path.join(
        Path.expand("../../../../_build/test-artifacts", __DIR__),
        "favn_user_owned_runner_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)

    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule Customer.MixProject do
        use Mix.Project
        def project, do: [app: :customer, version: "0.1.0"]
      end
      """
    )

    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, scaffold} = Runner.run(root_dir: root_dir, app: :customer)
    assert scaffold.target == :runner

    dockerfile = File.read!(Path.join(root_dir, "deploy/favn-runner/Dockerfile"))
    assert dockerfile =~ "ARG FAVN_RUNNER_RELEASE_ID"
    assert dockerfile =~ "io.favn.runner-release-id"
    refute dockerfile =~ "FROM favn"

    release_id = FavnTestSupport.runner_release_id(:alternate)

    assert {:ok, manifest} =
             Manifest.run(
               root_dir: root_dir,
               runner_release_id: release_id,
               allow_non_prod_build: true,
               skip_compile: true
             )

    assert manifest.required_runner_release_id == release_id
    assert {:ok, latest} = State.read_manifest_latest(root_dir: root_dir)
    assert latest["required_runner_release_id"] == release_id
    assert File.regular?(latest["manifest_path"])
  end
end
