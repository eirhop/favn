defmodule Favn.DeploymentArtifactsAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.CLI.Init
  alias FavnAuthoring.Deployment.ManifestBuilder

  @moduletag :acceptance

  test "deployment copy and empty manifest use the current release-map contract" do
    root_dir =
      Path.join(
        Path.expand("../../../../_build/test-artifacts", __DIR__),
        "favn_deployment_artifacts_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule Customer.MixProject do\nend\n")
    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, deployment} = Init.run(root_dir: root_dir, target: :deployment)

    dockerfile = File.read!(Path.join(deployment.output, "runner.Dockerfile"))
    assert dockerfile =~ "ARG FAVN_RUNNER_RELEASE_ID"
    assert dockerfile =~ "ARG FAVN_CUSTOMER_APP"
    assert dockerfile =~ "io.favn.runner-release-id"
    assert dockerfile =~ "mix release favn_runner --path /runner-release"
    assert dockerfile =~ ~s(*[!0-9a-f]*)

    compose = File.read!(Path.join(deployment.output, "compose.yml"))
    refute compose =~ "./postgres-ca.pem"
    assert compose =~ "FAVN_DATABASE_SSL_MODE:-disable"

    release_project = File.read!(Path.join(deployment.output, "mix.exs"))
    assert release_project =~ "@customer_app Application.compile_env"
    assert release_project =~ "{@customer_app, path: \"../..\"}"

    assert {:ok, manifest} =
             ManifestBuilder.run(
               root_dir: root_dir,
               runner_releases: %{},
               allow_non_prod_build: true,
               skip_compile: true
             )

    assert manifest.runner_releases == %{}
    assert manifest.status == :built
    assert File.regular?(manifest.manifest_path)
    refute File.exists?(Path.join([root_dir, ".favn", "dist", "manifest", "latest.json"]))

    assert {:ok, repeated} =
             ManifestBuilder.run(
               root_dir: root_dir,
               runner_releases: %{},
               allow_non_prod_build: true,
               skip_compile: true
             )

    assert repeated == %{manifest | status: :already_built}
  end
end
