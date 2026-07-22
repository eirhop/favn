defmodule Favn.Dev.InstallTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{Install, Lock, State}

  @version "0.5.0-dev"
  @build_id String.duplicate("a", 64)
  @image_id "sha256:" <> String.duplicate("b", 64)
  @digest "sha256:" <> String.duplicate("c", 64)
  @tag_reference "ghcr.io/eirhop/favn-control-plane:v0.5.0-dev"
  @immutable_reference "ghcr.io/eirhop/favn-control-plane@" <> @digest

  setup do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_compose_install_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "install resolves an official tag to a verified digest and writes Compose state", %{
    root_dir: root_dir
  } do
    runner = docker_runner(self())

    assert {:ok, :installed} =
             Install.run(
               root_dir: root_dir,
               favn_version: @version,
               docker_executable: "docker",
               docker_command_runner: runner
             )

    assert_received {:docker, ["pull", @tag_reference]}
    assert_received {:docker, ["image", "inspect", @tag_reference]}
    assert_received {:docker, ["image", "inspect", @immutable_reference]}

    assert {:ok, install} = State.read_install(root_dir: root_dir)
    assert install["schema_version"] == 4
    assert install["source"] == "official"
    assert install["favn_version"] == @version
    assert install["image_reference"] == @immutable_reference
    assert install["image_id"] == @image_id
    assert install["control_plane_build_id"] == @build_id

    project = install["compose"]
    assert project["project_name"] =~ ~r/\Afavn-[a-z0-9-]+-[0-9a-f]{12}\z/
    assert project["network_name"] == project["project_name"] <> "-network"
    assert project["postgres_volume_name"] == project["project_name"] <> "-postgres-data"

    compose = File.read!(project["compose_path"])
    assert compose =~ "image: ${FAVN_CONTROL_PLANE_IMAGE}"
    assert compose =~ "image: #{Favn.Dev.ComposeProject.postgres_image()}"
    assert compose =~ ~s("127.0.0.1:${FAVN_VIEW_PORT}:4000")
    assert compose =~ ~s("127.0.0.1:${FAVN_ORCHESTRATOR_PORT}:4101")
    assert compose =~ "aliases: [runner.favn.internal]"
    assert compose =~ "aliases: [control-plane.favn.internal]"
    assert compose =~ "FavnView.Readiness.liveness()"
    assert compose =~ "- ./runner.env"
    assert compose =~ "read_only: true"
    assert compose =~ "cap_drop: [ALL]"
    assert compose =~ "security_opt: [no-new-privileges:true]"

    assert compose =~
             "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION: ${FAVN_RUNTIME_INPUT_PIN_KEY_VERSION}"

    assert compose =~ "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS: ${FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS}"

    refute compose =~ "5432:5432"
    refute compose =~ "4369:4369"
    refute compose =~ "9100:9100"
    refute compose =~ root_dir
    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root"))

    env_path = project["env_path"]
    assert Bitwise.band(File.stat!(env_path).mode, 0o777) == 0o600
    assert Bitwise.band(File.stat!(project["postgres_init_path"]).mode, 0o777) == 0o555

    env = File.read!(env_path)
    assert env =~ "FAVN_CONTROL_PLANE_IMAGE='#{@immutable_reference}'"
    assert env =~ "FAVN_RUNNER_IMAGE='favn-local-runner-#{project["project_name"]}:unbuilt'"
    assert env =~ "FAVN_POSTGRES_RUNTIME_DATABASE_URL='ecto://favn_runtime:"
    assert env =~ "FAVN_RUNTIME_INPUT_PIN_KEY_VERSION='1'"
    assert env =~ "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS='120000'"
    assert env =~ "FAVN_WORKSPACE_NAME='Local Development'"
    assert env =~ "local-tooling-v1|platform_reader+platform_operator+platform_admin:"
    refute env =~ "replace-with"
    assert File.read!(project["runner_env_path"]) == ""
  end

  test "a matching installation uses the exact local digest without pulling again", %{
    root_dir: root_dir
  } do
    runner = docker_runner(self())
    opts = install_opts(root_dir, runner)

    assert {:ok, :installed} = Install.run(opts)
    assert_received {:docker, ["pull", @tag_reference]}

    assert {:ok, :already_installed} = Install.run(opts)
    refute_received {:docker, ["pull", @tag_reference]}
    assert :ok = Install.ensure_ready(opts)

    assert {:ok, :installed} = Install.run(Keyword.put(opts, :force, true))
    assert_received {:docker, ["pull", @tag_reference]}
  end

  test "install cannot interleave with another project mutation", %{root_dir: root_dir} do
    parent = self()

    holder =
      Task.async(fn ->
        Lock.with_lock([root_dir: root_dir], fn ->
          send(parent, :install_lock_acquired)

          receive do
            :release_install_lock -> :ok
          end
        end)
      end)

    assert_receive :install_lock_acquired, 2_000
    runner = docker_runner(self())

    assert {:error, {:lock_failed, :timeout}} =
             Install.run(
               install_opts(root_dir, runner)
               |> Keyword.put(:lock_timeout_ms, 0)
             )

    refute_received {:docker, _args}
    send(holder.pid, :release_install_lock)
    assert :ok = Task.await(holder)
  end

  test "a network pull failure reuses only the already verified exact digest", %{
    root_dir: root_dir
  } do
    working = docker_runner(self())
    assert {:ok, :installed} = Install.run(install_opts(root_dir, working))

    offline = docker_runner(self(), pull: {"network unavailable", 1})

    assert {:ok, :installed} =
             Install.run(install_opts(root_dir, offline) |> Keyword.put(:force, true))

    assert {:ok, install} = State.read_install(root_dir: root_dir)
    assert install["offline_reuse"] == true
    assert install["image_reference"] == @immutable_reference
  end

  test "missing Docker and registry failures leave no ready install state", %{root_dir: root_dir} do
    assert {:error, {:missing_tool, "docker"}} =
             Install.run(root_dir: root_dir, favn_version: @version, docker_executable: nil)

    assert {:error, :not_found} = State.read_install(root_dir: root_dir)

    auth_runner = docker_runner(self(), pull: {"unauthorized: authentication required", 1})

    assert {:error, :control_plane_registry_authentication_required} =
             Install.run(install_opts(root_dir, auth_runner))

    assert {:error, :not_found} = State.read_install(root_dir: root_dir)

    missing_runner = docker_runner(self(), pull: {"manifest unknown", 1})

    assert {:error, {:control_plane_version_unavailable, @tag_reference}} =
             Install.run(install_opts(root_dir, missing_runner))
  end

  test "incompatible labels fail closed before Compose state becomes ready", %{root_dir: root_dir} do
    runner =
      docker_runner(self(),
        inspection: image_inspection(%{"io.favn.runner-contract-version" => "999"})
      )

    assert {:error, :incompatible_control_plane_image} =
             Install.run(install_opts(root_dir, runner))

    assert {:error, :not_found} = State.read_install(root_dir: root_dir)
  end

  test "candidate injection is test-only and pins the exact local image ID", %{root_dir: root_dir} do
    runner = docker_runner(self())

    assert {:ok, :installed} =
             Install.run(
               install_opts(root_dir, runner) ++
                 [
                   candidate_control_plane: %{
                     "reference" => "favn-control-plane-candidate:#{@build_id}",
                     "image_id" => @image_id
                   }
                 ]
             )

    assert {:ok, install} = State.read_install(root_dir: root_dir)
    assert install["source"] == "candidate"
    assert install["image_reference"] == @image_id
    refute_received {:docker, ["pull", _reference]}
  end

  test "project identity is deterministic and rooted in the canonical path", %{root_dir: root_dir} do
    same = Path.join(root_dir, ".")
    other = root_dir <> "-other"

    assert Favn.Dev.ComposeProject.project_name(root_dir) ==
             Favn.Dev.ComposeProject.project_name(same)

    refute Favn.Dev.ComposeProject.project_name(root_dir) ==
             Favn.Dev.ComposeProject.project_name(other)
  end

  test "host feature probe accepts amd64 WSL2 and rejects unsupported hosts before Docker use", %{
    root_dir: root_dir
  } do
    runner = docker_runner(self())

    assert {:error, {:unsupported_docker_host, :darwin, "arm64"}} =
             Install.run(
               install_opts(root_dir, runner) ++
                 [
                   docker_host_platform: %{
                     os: :darwin,
                     architecture: "arm64",
                     environment: :darwin
                   }
                 ]
             )

    refute_received {:docker, _args}

    assert {:ok, :installed} =
             Install.run(
               install_opts(root_dir, runner) ++
                 [
                   docker_host_platform: %{
                     os: :linux,
                     architecture: "amd64",
                     environment: :wsl2
                   }
                 ]
             )
  end

  defp install_opts(root_dir, runner) do
    [
      root_dir: root_dir,
      favn_version: @version,
      docker_executable: "docker",
      docker_command_runner: runner
    ]
  end

  defp docker_runner(test_pid, overrides \\ []) do
    inspection = Keyword.get(overrides, :inspection, image_inspection())
    pull = Keyword.get(overrides, :pull, {"pulled", 0})

    fn "docker", args, _opts ->
      send(test_pid, {:docker, args})

      case args do
        ["version", "--format", "{{json .Server}}"] ->
          {JSON.encode!(%{"Os" => "linux", "Arch" => "amd64", "Version" => "28.3.0"}), 0}

        ["compose", "version", "--short"] ->
          {"2.39.1\n", 0}

        ["pull", @tag_reference] ->
          pull

        ["image", "inspect", _reference] ->
          {JSON.encode!([inspection]), 0}

        [
          "compose",
          "--project-name",
          _name,
          "--file",
          _file,
          "--env-file",
          _env,
          "config",
          "--quiet"
        ] ->
          {"", 0}

        other ->
          {"unexpected docker command: #{inspect(other)}", 97}
      end
    end
  end

  defp image_inspection(label_overrides \\ %{}) do
    labels =
      Map.merge(
        %{
          "org.opencontainers.image.version" => @version,
          "io.favn.control-plane.build-id" => @build_id,
          "io.favn.manifest-schema-version" =>
            Favn.Manifest.Compatibility.current_schema_version() |> Integer.to_string(),
          "io.favn.runner-contract-version" =>
            Favn.Manifest.Compatibility.current_runner_contract_version()
            |> Integer.to_string(),
          "io.favn.target" => "linux/amd64"
        },
        label_overrides
      )

    %{
      "Id" => @image_id,
      "RepoDigests" => [@immutable_reference],
      "Architecture" => "amd64",
      "Os" => "linux",
      "Config" => %{"User" => "10001:10001", "Labels" => labels}
    }
  end
end
