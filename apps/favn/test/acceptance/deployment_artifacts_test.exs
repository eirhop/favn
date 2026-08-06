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
    assert dockerfile =~ "io.favn.duckdb-version"
    assert dockerfile =~ "mix release favn_runner --path /runner-release"
    assert dockerfile =~ ~s(*[!0-9a-f]*)
    assert dockerfile =~ "DUCKDB_VERSION=1.5.5"
    assert dockerfile =~ "DUCKDB_ADBC_DRIVER=/opt/duckdb/${DUCKDB_VERSION}/libduckdb.so"
    assert dockerfile =~ "ducklake.duckdb_extension.gz"
    assert dockerfile =~ "postgres_scanner.duckdb_extension.gz"
    assert dockerfile =~ "json.duckdb_extension.gz"
    assert dockerfile =~ "LOAD ducklake; LOAD postgres; LOAD json"
    refute dockerfile =~ "FROM --platform"
    refute dockerfile =~ "--chown=10001:10001"
    assert dockerfile =~ "--mount=type=bind,source=.,target=/build,rw"
    assert dockerfile =~ "find / -xdev -type f -perm /6000 -exec chmod a-s"

    compose = File.read!(Path.join(deployment.output, "compose.yml"))
    refute compose =~ "./postgres-ca.pem"
    assert compose =~ "FAVN_DATABASE_SSL_MODE:-disable"
    assert compose =~ "read_only: true"
    assert compose =~ "no-new-privileges:true"
    assert compose =~ "cap_drop:"
    assert compose =~ "FAVN_LOG_LEVEL:-info"

    image_contract_path = Path.join(deployment.output, "runner-image-contract.sh")
    image_contract = File.read!(image_contract_path)
    assert image_contract =~ "EXPECTED_RUNNER_RELEASE_ID"
    assert image_contract =~ "--read-only"
    assert image_contract =~ "ducklake.duckdb_extension"
    assert {usage, 64} = System.cmd(image_contract_path, [], stderr_to_stdout: true)
    assert usage =~ "EXPECTED_RUNNER_RELEASE_ID"

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
