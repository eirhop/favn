defmodule Favn.Dev.InstallTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{Install, Lock, State}
  alias Favn.Dev.Maintainer.Candidate

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

  @tag :acceptance
  test "install resolves an official tag to image-only state without probing Compose", %{
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
    assert install["schema_version"] == 5
    assert install["source"] == "official"
    assert install["favn_version"] == @version
    assert install["image_reference"] == @immutable_reference
    assert install["image_id"] == @image_id
    assert install["control_plane_build_id"] == @build_id
    refute Map.has_key?(install, "compose")
    refute File.exists?(Path.join(root_dir, ".favn/install/runtime_root"))
    refute_received {:docker, ["compose" | _args]}
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
    refute_received {:docker, ["compose" | _args]}

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

  test "incompatible labels fail closed before install state becomes ready", %{root_dir: root_dir} do
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

  test "maintainer selection is local-only and normal install switches back to official", %{
    root_dir: root_dir
  } do
    runner = docker_runner(self())
    candidate = maintainer_candidate(root_dir)

    assert :ok =
             Install.select_maintainer(
               candidate,
               install_opts(root_dir, runner) ++ [allow_maintainer_install: true]
             )

    assert {:ok, maintainer} = State.read_install(root_dir: root_dir)
    assert maintainer["source"] == "maintainer"
    assert maintainer["image_reference"] == @image_id
    assert maintainer["checkout_revision"] == String.duplicate("d", 40)

    assert :ok =
             Install.ensure_ready(
               install_opts(root_dir, runner) ++ [allow_maintainer_install: true]
             )

    assert {:ok, :installed} = Install.run(install_opts(root_dir, runner))
    assert_received {:docker, ["pull", @tag_reference]}
    assert {:ok, %{"source" => "official"}} = State.read_install(root_dir: root_dir)
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

  defp maintainer_candidate(root_dir) do
    %Candidate{
      control_plane_build_id: @build_id,
      image_tag: "favn-control-plane-candidate:#{@build_id}",
      image_id: @image_id,
      candidate_path: Path.join(root_dir, "candidate.json"),
      image_source_revision: String.duplicate("d", 40),
      image_source_dirty: false,
      checkout: Path.join(root_dir, "favn"),
      checkout_revision: String.duplicate("d", 40),
      checkout_dirty: false,
      checkout_fingerprint: String.duplicate("e", 64)
    }
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
