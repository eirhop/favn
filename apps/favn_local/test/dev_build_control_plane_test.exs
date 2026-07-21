defmodule Favn.Dev.Build.ControlPlaneTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 180_000

  alias Favn.Dev.Build.Artifact
  alias Favn.Dev.Build.ControlPlane
  alias Favn.Dev.Build.ControlPlaneInputs
  alias Favn.ControlPlaneBuild

  @repo_root Path.expand("../../..", __DIR__)

  setup do
    build_root =
      Path.join(
        Path.join(@repo_root, "_build/test-artifacts"),
        "favn_control_plane_build_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(build_root) end)
    %{build_root: build_root}
  end

  test "collector includes only control-plane source and reachable production lock entries" do
    assert {:ok, collected} = ControlPlaneInputs.collect(@repo_root)

    paths = Enum.map(collected.descriptor.inputs, & &1.path)
    dependencies = collected.dependency_lock_apps

    assert collected.release_version == "0.5.0-dev"
    refute Map.has_key?(collected.descriptor.identity, "release_version")

    changed_release_metadata = %{collected | release_version: "0.6.0"}

    assert changed_release_metadata.descriptor.control_plane_build_id ==
             collected.descriptor.control_plane_build_id

    assert {:ok, changed_control_plane} =
             ControlPlaneBuild.new(
               collected.descriptor.inputs,
               Map.put(collected.descriptor.identity, "control_plane_version", "0.6.0")
             )

    refute changed_control_plane.control_plane_build_id ==
             collected.descriptor.control_plane_build_id

    assert "apps/favn_core/lib/favn/control_plane_build.ex" in paths
    assert "apps/favn_orchestrator/lib/favn_orchestrator/application.ex" in paths
    assert "apps/favn_storage_postgres/lib/favn_storage_postgres/release.ex" in paths
    assert "apps/favn_view/assets/css/app.css" in paths
    assert "rel/control_plane/Dockerfile" in paths
    assert "apps/favn_local/lib/favn/dev/build/artifact.ex" in paths
    assert "apps/favn_local/lib/favn/dev/build/control_plane.ex" in paths
    assert "apps/favn_local/lib/favn/dev/build/control_plane_inputs.ex" in paths
    assert "mix.lock/phoenix" in paths
    assert "mix.lock/postgrex" in paths
    assert "mix.lock/heroicons" in paths

    refute Enum.any?(paths, &String.starts_with?(&1, "apps/favn_runner/"))
    refute Enum.any?(paths, &String.starts_with?(&1, "apps/favn_local/test/"))
    refute "apps/favn_local/lib/favn/dev/run.ex" in paths
    refute Enum.any?(paths, &String.ends_with?(&1, ".md"))
    refute "duckdbex" in dependencies
    refute "adbc" in dependencies
    refute "tidewave" in dependencies
    refute "credo" in dependencies

    mutated_inputs =
      Enum.map(collected.descriptor.inputs, fn
        %{path: "apps/favn_local/lib/favn/dev/build/control_plane.ex"} = record ->
          %{record | sha256: String.duplicate("f", 64)}

        record ->
          record
      end)

    assert {:ok, changed_assembly} =
             ControlPlaneBuild.new(mutated_inputs, collected.descriptor.identity)

    refute changed_assembly.control_plane_build_id ==
             collected.descriptor.control_plane_build_id
  end

  test "explicit dependency roots fail closed when an app dependency drifts" do
    applications = [:favn_core]
    expected = [:jason]

    assert :ok =
             ControlPlaneInputs.validate_dependency_roots(
               [{:favn_core, path: "../favn_core"}, {:jason, "~> 1.4"}],
               applications,
               expected
             )

    assert {:error,
            {:control_plane_dependency_roots_mismatch,
             %{expected: [:jason], actual: [:jason, :new_runtime_dependency]}}} =
             ControlPlaneInputs.validate_dependency_roots(
               [
                 {:favn_core, path: "../favn_core"},
                 {:jason, "~> 1.4"},
                 {:new_runtime_dependency, "~> 1.0"}
               ],
               applications,
               expected
             )
  end

  test "lock identity follows required dependencies but ignores unrelated and optional entries" do
    root =
      {:hex, :root, "1.0.0", "checksum", [:mix],
       [
         {:required, "~> 1.0", [optional: false]},
         {:optional, "~> 1.0", [optional: true]}
       ], "hexpm", "outer"}

    required = {:hex, :required, "1.0.0", "required", [:mix], [], "hexpm", "outer"}
    optional = {:hex, :optional, "1.0.0", "optional", [:mix], [], "hexpm", "outer"}
    unrelated = {:hex, :unrelated, "1.0.0", "first", [:mix], [], "hexpm", "outer"}

    lock = %{
      "root" => root,
      "required" => required,
      "optional" => optional,
      "unrelated" => unrelated
    }

    assert {:ok, records, ["required", "root"]} = ControlPlaneInputs.lock_records(lock, [:root])
    refute Enum.any?(records, &(&1.path in ["mix.lock/optional", "mix.lock/unrelated"]))

    changed_unrelated = put_in(lock["unrelated"], put_elem(unrelated, 3, "second"))

    assert {:ok, ^records, ["required", "root"]} =
             ControlPlaneInputs.lock_records(changed_unrelated, [:root])

    changed_required = put_in(lock["required"], put_elem(required, 3, "changed"))

    assert {:ok, changed_records, ["required", "root"]} =
             ControlPlaneInputs.lock_records(changed_required, [:root])

    refute records == changed_records

    assert {:error, {:unresolved_control_plane_dependency, "missing"}} =
             ControlPlaneInputs.lock_records(lock, [:missing])
  end

  test "input discovery rejects symlinked files and directories" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_control_plane_inputs_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    File.mkdir_p!(Path.join(root, "source/nested"))
    File.write!(Path.join(root, "source/nested/input.ex"), "safe")
    File.ln_s!(Path.join(root, "source/nested/input.ex"), Path.join(root, "source/link.ex"))

    assert {:error, {:control_plane_input_symlink, "source/link.ex"}} =
             ControlPlaneInputs.regular_files(root, "source")

    File.rm!(Path.join(root, "source/link.ex"))
    File.ln_s!(Path.join(root, "source/nested"), Path.join(root, "source/link"))

    assert {:error, {:control_plane_input_symlink, "source/link"}} =
             ControlPlaneInputs.regular_files(root, "source")
  end

  test "input discovery prunes ignored dependency trees before symlink validation" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_control_plane_ignored_inputs_test_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    File.mkdir_p!(Path.join(root, "source/node_modules"))
    File.mkdir_p!(Path.join(root, "source/lib"))
    File.write!(Path.join(root, "source/lib/input.ex"), "safe")
    File.ln_s!(Path.join(root, "source/lib"), Path.join(root, "source/node_modules/link"))

    assert {:ok, ["source/lib/input.ex"]} = ControlPlaneInputs.regular_files(root, "source")
  end

  test "generated Phoenix digest outputs do not enter control-plane inputs" do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_control_plane_static_inputs_test_#{System.unique_integer([:positive])}"
      )

    fingerprint = String.duplicate("a", 32)

    files = [
      "apps/favn_view/priv/static/favicon.ico",
      "apps/favn_view/priv/static/images/logo.svg",
      "apps/favn_view/priv/static/cache_manifest.json",
      "apps/favn_view/priv/static/favicon-#{fingerprint}.ico",
      "apps/favn_view/priv/static/robots.txt.gz",
      "apps/favn_view/priv/static/assets/js/app.js"
    ]

    on_exit(fn -> File.rm_rf(root) end)

    Enum.each(files, fn relative ->
      path = Path.join(root, relative)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "fixture")
    end)

    assert {:ok, discovered} =
             ControlPlaneInputs.regular_files(root, "apps/favn_view/priv/static")

    selected = Enum.filter(discovered, &ControlPlaneInputs.context_input_path?/1)

    assert Enum.sort(selected) == [
             "apps/favn_view/priv/static/favicon.ico",
             "apps/favn_view/priv/static/images/logo.svg"
           ]
  end

  test "builder writes and verifies an isolated relocatable context", %{build_root: build_root} do
    source = Path.join(@repo_root, "rel/control_plane/env.sh.eex")
    source_mode = File.stat!(source).mode
    assert {:ok, before_mode_change} = ControlPlaneInputs.collect(@repo_root)
    File.chmod!(source, 0o755)
    on_exit(fn -> File.chmod!(source, Bitwise.band(source_mode, 0o777)) end)
    assert {:ok, after_mode_change} = ControlPlaneInputs.collect(@repo_root)

    assert before_mode_change.descriptor.control_plane_build_id ==
             after_mode_change.descriptor.control_plane_build_id

    assert {:ok, result} =
             ControlPlane.run(root_dir: @repo_root, build_root: build_root)

    assert result.status == :built
    assert result.control_plane_build_id =~ ~r/\A[0-9a-f]{64}\z/
    assert File.regular?(result.descriptor_path)
    assert File.regular?(Path.join(result.context_dir, "mix.exs"))
    assert File.regular?(Path.join(result.context_dir, "mix.lock"))
    assert File.regular?(Path.join(result.context_dir, "rel/control_plane/Dockerfile"))
    assert File.regular?(Path.join(result.context_dir, "bundle.json"))

    for application <- ~w(favn_core favn_orchestrator favn_storage_postgres favn_view) do
      assert File.dir?(Path.join(result.context_dir, "apps/#{application}"))
    end

    refute File.exists?(Path.join(result.context_dir, "apps/favn_runner"))
    refute File.exists?(Path.join(result.context_dir, "apps/favn_local"))
    refute File.exists?(Path.join(result.context_dir, "apps/favn_authoring"))
    refute File.exists?(Path.join(result.context_dir, "apps/favn_view/test"))

    assert Bitwise.band(File.stat!(result.context_dir).mode, 0o777) == 0o755

    assert Bitwise.band(
             File.stat!(Path.join(result.context_dir, "rel/control_plane/env.sh.eex")).mode,
             0o777
           ) == 0o644

    assert Bitwise.band(File.stat!(result.descriptor_path).mode, 0o777) == 0o644

    dockerfile = File.read!(Path.join(result.context_dir, "rel/control_plane/Dockerfile"))
    assert dockerfile =~ "mix deps.get --only prod --check-locked"
    assert dockerfile =~ "tailwind-linux-x64-4.1.12"
    assert dockerfile =~ "5eeee66ea237eae9a160fa3314fd0cf76ab993551a99fafb16fa1db6c6b90289"
    assert dockerfile =~ "esbuild-linux-x64"
    assert dockerfile =~ "93433b456cac3a454ee27403d3de9adce88d83e5439ba37e1471af54730c9ca7"

    context_lock = Mix.Dep.Lock.read(Path.join(result.context_dir, "mix.lock"))
    descriptor = result.descriptor_path |> File.read!() |> JSON.decode!()

    assert context_lock |> Map.keys() |> Enum.map(&to_string/1) |> Enum.sort() ==
             descriptor["identity"]["dependency_lock_apps"]

    refute Map.has_key?(context_lock, :duckdbex)
    refute Map.has_key?(context_lock, :adbc)

    assert :ok =
             Artifact.verify_bundle(result.context_dir, "favn_control_plane_context", %{
               "control_plane_build_id" => result.control_plane_build_id
             })

    assert {:ok, same} =
             ControlPlane.run(root_dir: @repo_root, build_root: build_root)

    assert same.status == :already_built
    assert same.control_plane_build_id == result.control_plane_build_id
  end

  test "corrupted immutable contexts fail closed", %{build_root: build_root} do
    assert {:ok, result} =
             ControlPlane.run(root_dir: @repo_root, build_root: build_root)

    File.write!(Path.join(result.context_dir, "config/runtime.exs"), "corrupted")

    assert {:error, {:control_plane_artifact_conflict, :invalid_control_plane_artifact}} =
             ControlPlane.run(root_dir: @repo_root, build_root: build_root)
  end

  test "control-plane release launcher rejects unsafe distribution values" do
    release_env = Path.join(@repo_root, "rel/control_plane/env.sh.eex")
    valid_cookie = "favn-control-cookie-7A9c2D4e6F8h0J1k"

    assert {"", 0} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_CONTROL_PLANE_NODE", "control@control.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9101"}
               ],
               stderr_to_stdout: true
             )

    for invalid_node <- ["control@localhost", "control@127.2.3.4", "control@@internal", "control"] do
      assert {output, 1} =
               System.cmd("sh", [release_env],
                 env: [
                   {"FAVN_CONTROL_PLANE_NODE", invalid_node},
                   {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                   {"FAVN_BEAM_DISTRIBUTION_PORT", "9101"}
                 ],
                 stderr_to_stdout: true
               )

      assert output =~ "invalid FAVN_CONTROL_PLANE_NODE"
    end

    assert {output, 1} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_CONTROL_PLANE_NODE", "control@control.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", String.duplicate("z", 32)},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9101"}
               ],
               stderr_to_stdout: true
             )

    assert output =~ "invalid FAVN_DISTRIBUTION_COOKIE"
  end

  @tag :slow
  @tag timeout: 1_200_000
  test "loaded candidate is a minimal Linux amd64 non-root release image", %{
    build_root: build_root
  } do
    assert {:ok, result} =
             ControlPlane.run(root_dir: @repo_root, build_root: build_root, load: true)

    assert result.image_status == :loaded

    assert result.image_tag ==
             "favn-control-plane-candidate:#{result.control_plane_build_id}"

    assert result.image_id =~ ~r/\Asha256:[0-9a-f]{64}\z/
    assert result.static_asset_digest =~ ~r/\A[0-9a-f]{64}\z/
    assert File.regular?(result.candidate_path)

    docker =
      System.find_executable("docker") || flunk("Docker is required for the slow image test")

    assert {"linux/amd64\n", 0} =
             System.cmd(
               docker,
               ["image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", result.image_tag]
             )

    assert {"10001:10001\n", 0} =
             System.cmd(docker, [
               "image",
               "inspect",
               "--format",
               "{{.Config.User}}",
               result.image_tag
             ])

    contract = """
    set -eu
    test "$(id -u)" = 10001
    test -x /app/bin/favn_control_plane
    test -x /app/bin/favn_control_plane_health
    test -x /app/bin/favn_control_plane_ops
    test -f /app/control-plane-build.json
    test "$(cat /app/runtime-versions/ELIXIR_VERSION)" = 1.20.2
    test "$(cat /app/runtime-versions/OTP_VERSION)" = 28.3.3
    test ! -e /app/releases/COOKIE
    ! find /app -type f \( -name COOKIE -o -name .erlang.cookie \) | grep -q .
    test -d /app/lib/favn_view-0.1.0
    test -d /app/lib/favn_orchestrator-0.5.0-dev
    test -d /app/lib/favn_storage_postgres-0.5.0-dev
    ! find /app/lib -maxdepth 1 -type d -name 'favn_runner-*' | grep -q .
    ! find /app/lib -maxdepth 1 -type d -name 'favn_local-*' | grep -q .
    ! find /app/lib -maxdepth 1 -type d -name 'favn_authoring-*' | grep -q .
    ! find /app -type f -name '*.ex' | grep -q .
    ! find /app -type f -name '*.exs' ! -path '/app/releases/*/runtime.exs' | grep -q .
    ! find /app -type f \( -name '*.eex' -o -name '*.heex' \) | grep -q .
    ! find /app -type f -name '*.map' | grep -q .
    ! grep -R -l '"sourcesContent"' /app | grep -q .
    ! find /app/lib -path '*/phoenix-*/priv/templates' -type d | grep -q .
    ! find /app -type f -name 'mix.exs' | grep -q .
    ! grep -F '/build/' /app/releases/*/sys.config | grep -q .
    ! grep -E '\{(esbuild|tailwind),' /app/releases/*/sys.config | grep -q .
    """

    assert {_output, 0} =
             System.cmd(
               docker,
               [
                 "run",
                 "--rm",
                 "--read-only",
                 "--tmpfs",
                 "/tmp:rw,noexec,nosuid,size=64m,uid=10001,gid=10001,mode=0700",
                 "--entrypoint",
                 "/bin/sh",
                 result.image_tag,
                 "-c",
                 contract
               ],
               stderr_to_stdout: true
             )
  end
end
