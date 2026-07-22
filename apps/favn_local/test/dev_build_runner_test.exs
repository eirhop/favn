defmodule Favn.Dev.Build.RunnerTest do
  use ExUnit.Case, async: false

  alias Favn.Dev
  alias Favn.Dev.Build.Artifact
  alias Favn.Dev.Build.RunnerInputs
  alias Favn.Dev.Build.RunnerReleaseInput
  alias Favn.RunnerRelease

  @control_build_id String.duplicate("a", 64)
  @control_image_id "sha256:" <> String.duplicate("b", 64)

  defmodule SQLVersionOne do
    use Favn.SQLAsset

    materialized(:table)
    relation(connection: :warehouse)

    query do
      ~SQL"select 1 as version"
    end
  end

  defmodule SQLVersionTwo do
    use Favn.SQLAsset

    materialized(:table)
    relation(connection: :warehouse)

    query do
      ~SQL"select 2 as version"
    end
  end

  setup do
    root_dir =
      Path.join("/tmp", "favn_dev_build_runner_test_#{System.unique_integer([:positive])}")

    for app <- ~w(favn_runner favn_orchestrator favn_view) do
      File.mkdir_p!(Path.join(root_dir, "apps/#{app}"))
      File.write!(Path.join(root_dir, "apps/#{app}/mix.exs"), "defmodule Fixture do end")
    end

    File.write!(Path.join(root_dir, "mix.lock"), "lock")

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "requires the host compiler to match the pinned runner compiler" do
    expected = %{elixir_version: "1.20.2", otp_release: "29"}

    assert RunnerReleaseInput.expected_toolchain() == expected
    assert :ok = RunnerReleaseInput.validate_toolchain(expected)

    assert {:error,
            {:runner_build_toolchain_mismatch, ^expected,
             %{elixir_version: "1.20.2", otp_release: "28"}}} =
             RunnerReleaseInput.validate_toolchain(%{
               elixir_version: "1.20.2",
               otp_release: "28"
             })
  end

  test "build_runner/1 writes an immutable relocatable OCI context", %{root_dir: root_dir} do
    install!(root_dir)

    assert {:ok, result} = build(root_dir)
    assert result.status == :built
    assert result.build_id == result.runner_release_id
    assert Path.basename(result.dist_dir) == result.runner_release_id

    assert {:ok, latest} = Favn.Dev.State.read_runner_latest(root_dir: root_dir)
    assert latest["runner_release_id"] == result.runner_release_id
    assert latest["descriptor_path"] == result.descriptor_path
    assert latest["manifest_dir"] == result.manifest_dir

    expected = [
      "Dockerfile",
      "bundle.json",
      "runner-release.json",
      "operator-notes.md",
      "manifest/bundle.json",
      "manifest/manifest-index.json",
      "dependency-input/mix.exs",
      "dependency-input/mix.lock",
      "dependency-input/dependency-input.json",
      "dependency-input/apps/favn_runner/mix.exs",
      "application-input/mix.exs",
      "application-input/dependency-lock.json",
      "application-input/stamp_apps.exs",
      "application-input/apps/favn_local/mix.exs",
      "application-input/runner-priv/runner-release.json"
    ]

    assert Enum.all?(expected, &File.exists?(Path.join(result.dist_dir, &1)))
    assert File.dir?(Path.join(result.dist_dir, "manifest/execution-packages"))

    assert {:ok, descriptor_bytes} = File.read(result.descriptor_path)
    assert {:ok, descriptor} = RunnerRelease.decode(descriptor_bytes)
    assert descriptor.runner_release_id == result.runner_release_id
    assert Enum.any?(descriptor.runtime_applications, &(&1.application == "favn_local"))

    dependency_identity =
      result.dist_dir
      |> Path.join("dependency-input/dependency-input.json")
      |> File.read!()
      |> JSON.decode!()

    assert dependency_identity["digest"] =~ ~r/\A[0-9a-f]{64}\z/
    refute Enum.any?(dependency_identity["files"], &String.contains?(&1["path"], "favn_local"))

    assert {:ok, bundle_bytes} = File.read(Path.join(result.dist_dir, "bundle.json"))
    assert {:ok, bundle} = JSON.decode(bundle_bytes)
    assert bundle["schema_version"] == 2
    assert bundle["runner_release_id"] == result.runner_release_id
    assert bundle["kind"] == "favn_runner_build_context"
    assert Enum.any?(bundle["files"], &(&1["path"] == "manifest/bundle.json"))
    refute bundle_bytes =~ root_dir

    dockerfile = File.read!(Path.join(result.dist_dir, "Dockerfile"))
    assert dockerfile =~ "@sha256:"
    assert dockerfile =~ "FROM --platform=linux/amd64 "
    assert dockerfile =~ "AS toolchain"
    assert dockerfile =~ "FROM toolchain AS dependencies"
    assert dockerfile =~ "FROM dependencies AS builder"
    assert length(Regex.scan(~r/^FROM /m, dockerfile)) == 4
    assert dockerfile =~ "USER 10001:10001"
    assert dockerfile =~ ~s(ENTRYPOINT ["/opt/favn/bin/favn_runner"])
    assert dockerfile =~ "RUN rm -f /opt/favn/releases/COOKIE"
    assert dockerfile =~ "snapshot.debian.org/archive/debian/20260713T000000Z"
    assert dockerfile =~ "mix local.hex 2.5.1 --force"
    assert dockerfile =~ "rebar3/releases/download/3.27.0/rebar3"
    assert dockerfile =~ "mix deps.get --only prod --check-locked"
    assert dockerfile =~ "--mount=type=cache,target=/root/.hex"
    assert dockerfile =~ "COPY dependency-input/ ./"
    assert dockerfile =~ "COPY application-input/apps/ ./apps/"
    assert dockerfile =~ "mkdir -p /var/lib/favn/data"
    refute dockerfile =~ "VOLUME [\"/var/lib/favn/data\"]"
    assert dockerfile =~ "io.favn.elixir-version=\"1.20.2\""
    assert dockerfile =~ "io.favn.otp-version=\"29.0.3\""
    assert dockerfile =~ "LANG=C.UTF-8 LC_ALL=C.UTF-8"
    refute dockerfile =~ root_dir

    source_fixture = Path.join(root_dir, "debian.sources")

    File.write!(
      source_fixture,
      "URIs: http://deb.debian.org/debian\nURIs: http://deb.debian.org/debian-security\n"
    )

    sed_expressions =
      Regex.scan(~r/-e '([^']+)'/, dockerfile, capture: :all_but_first)
      |> Enum.take(2)
      |> Enum.flat_map(fn [expression] -> ["-e", expression] end)

    assert {"", 0} = System.cmd("sed", ["-i" | sed_expressions] ++ [source_fixture])

    assert File.read!(source_fixture) ==
             "URIs: http://snapshot.debian.org/archive/debian/20260713T000000Z\n" <>
               "URIs: http://snapshot.debian.org/archive/debian-security/20260713T000000Z\n"

    release_env = Path.join(result.dist_dir, "application-input/rel/env.sh.eex")
    valid_cookie = "favn-runner-cookie-7A9c2D4e6F8h0J1k"

    assert {"", 0} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_RUNNER_NODE", "runner@runner.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"}
               ],
               stderr_to_stdout: true
             )

    for {release_command, expected} <- [
          {"start", "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9100"},
          {"rpc", ""}
        ] do
      assert {erl_aflags, 0} =
               System.cmd(
                 "sh",
                 ["-c", ~s(. "$1"; printf '%s' "${ERL_AFLAGS:-}"), "sh", release_env],
                 env: [
                   {"RELEASE_COMMAND", release_command},
                   {"ERL_AFLAGS", ""},
                   {"FAVN_RUNNER_NODE", "runner@runner.internal"},
                   {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                   {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"}
                 ],
                 stderr_to_stdout: true
               )

      assert String.trim(erl_aflags) == expected
    end

    for invalid_node <- ["runner@localhost", "runner@127.0.0.2", "runner@@internal", "runner"] do
      assert {output, 1} =
               System.cmd("sh", [release_env],
                 env: [
                   {"FAVN_RUNNER_NODE", invalid_node},
                   {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                   {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"}
                 ],
                 stderr_to_stdout: true
               )

      assert output =~ "invalid FAVN_RUNNER_NODE"
    end

    assert {output, 1} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_RUNNER_NODE", "runner@runner.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", String.duplicate("a", 32)},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"}
               ],
               stderr_to_stdout: true
             )

    assert output =~ "invalid FAVN_DISTRIBUTION_COOKIE"

    assert {"", 0} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_RUNNER_NODE", "runner@runner.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"},
                 {"ERL_EPMD_PORT", "4370"}
               ],
               stderr_to_stdout: true
             )

    assert {output, 1} =
             System.cmd("sh", [release_env],
               env: [
                 {"FAVN_RUNNER_NODE", "runner@runner.internal"},
                 {"FAVN_DISTRIBUTION_COOKIE", valid_cookie},
                 {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"},
                 {"ERL_EPMD_PORT", "invalid"}
               ],
               stderr_to_stdout: true
             )

    assert output =~ "invalid ERL_EPMD_PORT"

    for relative <- [
          "dependency-input/mix.exs",
          "dependency-input/config/config.exs",
          "application-input/mix.exs",
          "application-input/stamp_apps.exs"
        ] do
      assert {:ok, _quoted} =
               result.dist_dir
               |> Path.join(relative)
               |> File.read!()
               |> Code.string_to_quoted(file: relative)
    end

    evaluated_config =
      result.dist_dir
      |> Path.join("dependency-input/config/config.exs")
      |> Config.Reader.read!()

    assert is_list(Keyword.get(evaluated_config, :favn))

    assert File.read!(Path.join(result.dist_dir, "application-input/mix.exs")) =~
             "runtime: false"

    assert {:ok, same} = build(root_dir)
    assert same.status == :already_built
    assert same.runner_release_id == result.runner_release_id
    assert same.dist_dir == result.dist_dir

    assert {:ok, manifest_result} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain()
             )

    assert manifest_result.required_runner_release_id == result.runner_release_id
    assert File.exists?(manifest_result.manifest_path)

    File.write!(manifest_result.manifest_path, "tampered")

    assert {:error, :artifact_bundle_invalid} =
             Artifact.verify_bundle(manifest_result.dist_dir, "favn_manifest_release")
  end

  test "runner lock keeps non-vendored transitive build dependencies only" do
    lock = Mix.Dep.Lock.read()

    selected =
      Map.take(lock, [:duckdbex, :cc_precompiler, :elixir_make, :phoenix])

    assert {:ok, release_lock} =
             RunnerInputs.select_dependency_lock(
               selected,
               ["duckdbex"],
               %{
                 "duckdbex" => ["cc_precompiler"],
                 "cc_precompiler" => ["elixir_make"],
                 "elixir_make" => []
               }
             )

    assert Map.keys(release_lock) |> Enum.sort() == ["cc_precompiler", "elixir_make"]
  end

  test "build_runner/1 fails closed when an immutable artifact was modified", %{
    root_dir: root_dir
  } do
    install!(root_dir)
    assert {:ok, result} = build(root_dir)
    File.write!(Path.join(result.dist_dir, "Dockerfile"), "tampered")

    assert {:error, :runner_artifact_conflict} = build(root_dir)
  end

  test "runner_build dynamic roots require a new runner", %{root_dir: root_dir} do
    install!(root_dir)
    previous = Application.get_env(:favn, :runner_build)
    on_exit(fn -> restore_env(:runner_build, previous) end)

    Application.delete_env(:favn, :runner_build)
    assert {:ok, result} = build(root_dir)

    Application.put_env(:favn, :runner_build,
      extra_modules: [Favn.Dev.Paths],
      extra_applications: []
    )

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain()
             )

    assert :runtime_code in categories
    assert {:ok, replacement} = build(root_dir)
    refute replacement.runner_release_id == result.runner_release_id
  end

  test "a SQL-only edit reuses the runner and writes the current standalone manifest", %{
    root_dir: root_dir
  } do
    install!(root_dir)
    previous = Application.get_env(:favn, :asset_modules)
    on_exit(fn -> restore_env(:asset_modules, previous) end)

    Application.put_env(:favn, :asset_modules, [SQLVersionOne])
    assert {:ok, first} = build(root_dir)
    assert first.status == :built
    first_manifest_dir = first.manifest_dir

    Application.put_env(:favn, :asset_modules, [SQLVersionTwo])
    assert {:ok, second} = build(root_dir)
    assert second.status == :already_built
    assert second.runner_release_id == first.runner_release_id
    refute second.manifest_dir == first_manifest_dir
    assert second.manifest_status == :built
    assert File.regular?(Path.join(second.manifest_dir, "manifest-index.json"))
    assert second.embedded_manifest_dir == Path.join(first.dist_dir, "manifest")
  end

  test "baked runner configuration participates in immutable release identity", %{
    root_dir: root_dir
  } do
    install!(root_dir)
    previous = Application.get_env(:favn, :execution_pools)
    on_exit(fn -> restore_env(:execution_pools, previous) end)

    Application.put_env(:favn, :execution_pools, default: [size: 1])
    assert {:ok, first} = build(root_dir)

    Application.put_env(:favn, :execution_pools, default: [size: 2])
    assert {:ok, second} = build(root_dir)
    refute second.runner_release_id == first.runner_release_id
    refute second.dist_dir == first.dist_dir
  end

  test "manifest-only customer settings do not change runner identity", %{root_dir: root_dir} do
    install!(root_dir)
    customer = Path.join(root_dir, "settings-only-source")
    File.mkdir_p!(Path.join(customer, "config"))
    File.mkdir_p!(Path.join(customer, "lib"))
    File.write!(Path.join(customer, "mix.exs"), "defmodule SettingsOnly.MixProject do end")

    config = Path.join(customer, "config/config.exs")
    File.write!(config, "import Config\nconfig :favn, :asset_modules, []\n")
    assert {:ok, first} = build(root_dir, current_app_source: customer)

    File.write!(config, "import Config\nconfig :favn, :asset_modules, [:manifest_only]\n")
    assert {:ok, second} = build(root_dir, current_app_source: customer)
    assert second.runner_release_id == first.runner_release_id
    assert second.status == :already_built
  end

  test "runner_build rejects unknown keys and non-atom entries", %{root_dir: root_dir} do
    install!(root_dir)
    previous = Application.get_env(:favn, :runner_build)
    on_exit(fn -> restore_env(:runner_build, previous) end)

    for config <- [
          [unknown: []],
          [extra_modules: ["Elixir.MyApp.Dynamic"]],
          [extra_applications: ["my_runtime_app"]]
        ] do
      Application.put_env(:favn, :runner_build, config)
      assert {:error, :invalid_runner_build_config} = build(root_dir)
    end
  end

  test "a customer lock change conservatively requires a new runner", %{root_dir: root_dir} do
    install!(root_dir)
    first_lock = Map.put(Mix.Dep.Lock.read(), :authoring_only, {:hex, :one})
    second_lock = Map.put(Mix.Dep.Lock.read(), :authoring_only, {:hex, :two})

    assert {:ok, result} = build(root_dir, lock: first_lock)

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain(),
               lock: second_lock
             )

    assert :runtime_dependencies in categories
  end

  test "vendors customer application config, runtime config, and priv resources", %{
    root_dir: root_dir
  } do
    install!(root_dir)
    customer = Path.join(root_dir, "customer-source")
    File.mkdir_p!(Path.join(customer, "config"))
    File.mkdir_p!(Path.join(customer, "priv"))
    File.mkdir_p!(Path.join(customer, "priv/docs"))
    File.mkdir_p!(Path.join(customer, "priv/test"))
    File.mkdir_p!(Path.join(customer, "lib"))
    File.write!(Path.join(customer, "mix.exs"), "defmodule Customer.MixProject do end")

    File.write!(
      Path.join(customer, "config/config.exs"),
      "import Config\nconfig :customer, :mode, :prod\n"
    )

    File.write!(
      Path.join(customer, "config/dev.exs"),
      "import Config\nconfig :customer, :dev, true\n"
    )

    File.write!(
      Path.join(customer, "config/runtime.exs"),
      "import Config\nconfig :customer, :endpoint, System.get_env(\"CUSTOMER_ENDPOINT\")\n"
    )

    File.write!(Path.join(customer, "priv/resource.txt"), "customer-resource")
    File.write!(Path.join(customer, "priv/docs/schema.json"), "{}")
    File.write!(Path.join(customer, "priv/test/fixture.bin"), "fixture")

    assert {:ok, result} = build(root_dir, current_app_source: customer)

    refute File.read!(Path.join(result.dist_dir, "dependency-input/config/config.exs")) =~
             "import_config"

    assert File.read!(Path.join(result.dist_dir, "application-input/config/runtime.exs")) =~
             ~s(import_config "customer_config/runtime.exs")

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "application-input/rel/overlays/releases/1.0.0/customer_config/runtime.exs"
             )
           )

    refute File.exists?(
             Path.join(
               result.dist_dir,
               "application-input/rel/overlays/releases/1.0.0/customer_config/dev.exs"
             )
           )

    assert File.read!(
             Path.join(result.dist_dir, "application-input/apps/favn_local/priv/resource.txt")
           ) == "customer-resource"

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "application-input/apps/favn_local/priv/docs/schema.json"
             )
           )

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "application-input/apps/favn_local/priv/test/fixture.bin"
             )
           )

    resource = Path.join(customer, "priv/resource.txt")
    File.chmod!(resource, 0o755)
    assert {:ok, executable_result} = build(root_dir, current_app_source: customer)
    refute executable_result.runner_release_id == result.runner_release_id

    assert dependency_input_digest(executable_result) == dependency_input_digest(result)

    copied_resource =
      Path.join(
        executable_result.dist_dir,
        "application-input/apps/favn_local/priv/resource.txt"
      )

    assert Bitwise.band(File.stat!(copied_resource).mode, 0o111) != 0

    File.write!(resource, "changed-resource")

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               current_app_source: customer,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain()
             )

    assert :runtime_dependencies in categories
  end

  test "dependency source changes invalidate the deterministic dependency input", %{
    root_dir: root_dir
  } do
    sources = dependency_sources()
    original = Map.fetch!(sources, :favn_runner)
    changed = Path.join(root_dir, "changed-favn-runner")
    assert :ok = Artifact.copy_tree(original, changed)

    assert {:ok, first} =
             build(root_dir, dependency_sources: Map.put(sources, :favn_runner, changed))

    File.mkdir_p!(Path.join(changed, "priv"))
    File.write!(Path.join(changed, "priv/dependency-cache-key"), "changed")

    assert {:ok, second} =
             build(root_dir, dependency_sources: Map.put(sources, :favn_runner, changed))

    refute first.runner_release_id == second.runner_release_id
    refute dependency_input_digest(first) == dependency_input_digest(second)
  end

  test "rejects secret-bearing source files before writing a context", %{root_dir: root_dir} do
    install!(root_dir)
    customer = Path.join(root_dir, "secret-source")
    File.mkdir_p!(Path.join(customer, "lib"))
    File.write!(Path.join(customer, "mix.exs"), "defmodule Secret.MixProject do end")
    File.write!(Path.join(customer, ".env"), "CUSTOMER_TOKEN=must-not-be-copied")

    assert {:error, {:sensitive_source_file, ".env"}} =
             build(root_dir, current_app_source: customer)

    assert Path.wildcard(Path.join(root_dir, ".favn/dist/runner/*")) == []
  end

  test "rejects runtime config import chains that cannot survive the final image", %{
    root_dir: root_dir
  } do
    install!(root_dir)
    customer = Path.join(root_dir, "runtime-import-source")
    File.mkdir_p!(Path.join(customer, "config"))
    File.mkdir_p!(Path.join(customer, "lib"))
    File.write!(Path.join(customer, "mix.exs"), "defmodule RuntimeImport.MixProject do end")

    File.write!(
      Path.join(customer, "config/runtime.exs"),
      "import Config\nimport_config \"prod.exs\"\n"
    )

    File.write!(Path.join(customer, "config/prod.exs"), "import Config\n")

    assert {:error, :customer_runtime_config_imports_unsupported} =
             build(root_dir, current_app_source: customer)
  end

  test "production artifacts reject non-production compilation by default", %{root_dir: root_dir} do
    assert {:error, {:production_build_required, :test}} =
             Dev.build_runner(
               root_dir: root_dir,
               skip_compile: true,
               skip_project_root_check: true
             )
  end

  test "build_runner/1 fingerprints configured plugins", %{root_dir: root_dir} do
    install!(root_dir)
    previous = Application.get_env(:favn, :runner_plugins)

    Application.put_env(:favn, :runner_plugins, [
      {Favn.Runner.SupervisedChildren, children: []}
    ])

    on_exit(fn -> restore_env(:runner_plugins, previous) end)

    assert {:ok, result} = build(root_dir)
    assert {:ok, descriptor} = result.descriptor_path |> File.read!() |> RunnerRelease.decode()

    assert Enum.map(descriptor.plugins, & &1.plugin) == [
             "Elixir.Favn.Runner.SupervisedChildren"
           ]

    Application.delete_env(:favn, :runner_plugins)

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain()
             )

    assert :plugins in categories
  end

  test "build_runner/1 is independent of installation and Compose", %{root_dir: root_dir} do
    assert {:ok, result} = build(root_dir)
    assert result.status == :built
    refute File.exists?(Path.join(root_dir, ".favn/install.json"))
  end

  @tag :slow
  @tag timeout: 1_200_000
  test "relocated generated context builds and self-verifies a protocol-bearing release", %{
    root_dir: root_dir
  } do
    install!(root_dir)

    assert {:ok, result} =
             build(root_dir, extra_modules: [Inspect.Favn.RuntimeValue.Ref])

    assert {:ok, descriptor} = result.descriptor_path |> File.read!() |> RunnerRelease.decode()

    assert Enum.any?(
             descriptor.runtime_modules,
             &(&1.module == "Elixir.Inspect.Favn.RuntimeValue.Ref")
           )

    relocated = Path.join(root_dir, "relocated-runner-context")
    assert :ok = Artifact.copy_tree(result.dist_dir, relocated)
    assert {:ok, _removed} = File.rm_rf(result.dist_dir)

    image = "favn-runner-release-test:#{System.unique_integer([:positive])}"

    on_exit(fn ->
      _ = System.cmd("docker", ["image", "rm", "--force", image], stderr_to_stdout: true)
    end)

    assert {build_output, 0} =
             System.cmd(
               "docker",
               ["build", "--tag", image, relocated],
               stderr_to_stdout: true,
               env: [{"DOCKER_BUILDKIT", "1"}]
             )

    refute build_output =~ "runner release verification failed"

    inspect_format =
      ~s({{.Config.User}}|{{.Architecture}}|{{ index .Config.Labels "io.favn.runner-release-id" }}|{{ index .Config.Labels "io.favn.elixir-version" }}|{{ index .Config.Labels "io.favn.otp-version" }})

    assert {image_metadata, 0} =
             System.cmd(
               "docker",
               ["image", "inspect", "--format", inspect_format, image],
               stderr_to_stdout: true
             )

    assert String.trim(image_metadata) ==
             "10001:10001|amd64|#{descriptor.runner_release_id}|1.20.2|29.0.3"

    assert {_output, 0} =
             System.cmd(
               "docker",
               [
                 "run",
                 "--rm",
                 "--entrypoint",
                 "/bin/sh",
                 image,
                 "-c",
                 "set -- /opt/favn/lib/favn_local-*/ebin/Elixir.Favn.Dev.Build.Runner.beam; test ! -e \"$1\"; test ! -e /opt/favn/releases/COOKIE; ! find /opt/favn -type f -name .erlang.cookie | grep -q .; test \"$(cat /opt/favn/runtime-versions/ELIXIR_VERSION)\" = 1.20.2; test \"$(cat /opt/favn/runtime-versions/OTP_VERSION)\" = 29.0.3"
               ],
               stderr_to_stdout: true
             )

    cookie = "favn-runner-test-cookie-7A9c2D4e6F8h0J1k"

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "run",
                 "--rm",
                 "--read-only",
                 "--tmpfs",
                 "/tmp/favn:rw,noexec,nosuid,nodev,uid=10001,gid=10001,mode=0700",
                 "--hostname",
                 "runner.internal",
                 "--env",
                 "FAVN_RUNNER_NODE=favn_runner@runner.internal",
                 "--env",
                 "FAVN_CONTROL_PLANE_NODE=favn_control@control.internal",
                 "--env",
                 "FAVN_DISTRIBUTION_COOKIE=#{cookie}",
                 "--env",
                 "FAVN_BEAM_DISTRIBUTION_PORT=9100",
                 image,
                 "eval",
                 "IO.inspect(Application.ensure_all_started(:favn_runner), label: :started); IO.inspect(FavnRunner.release_info(), label: :release_info)"
               ],
               stderr_to_stdout: true
             )

    assert output =~ "started: {:ok"
    assert output =~ descriptor.runner_release_id
    refute output =~ "native name encoding of latin1"
  end

  test "failed validation leaves no publishable artifact", %{root_dir: root_dir} do
    install!(root_dir)
    previous = Application.get_env(:favn, :connections)
    Application.put_env(:favn, :connections, warehouse: [password: "literal-secret"])
    on_exit(fn -> restore_env(:connections, previous) end)

    assert {:error, {:secret_literal_in_runner_config, [:connections, :warehouse, :password]}} =
             build(root_dir)

    assert Path.wildcard(Path.join(root_dir, ".favn/dist/runner/*")) == []

    descriptor_path = Path.join(root_dir, "invalid-runner-release.json")
    {:ok, encoded} = RunnerRelease.encode(FavnTestSupport.runner_release())
    invalid = encoded |> JSON.decode!() |> Map.put("schema_version", 999) |> JSON.encode!()
    File.write!(descriptor_path, invalid)

    assert {:error, {:runner_release_descriptor_invalid, :unsupported_schema}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               host_toolchain: RunnerReleaseInput.expected_toolchain()
             )

    assert Path.wildcard(Path.join(root_dir, ".favn/dist/manifest/*")) == []
  end

  test "runner Docker labels accept only bounded source revisions" do
    assert {:ok, "unknown"} = RunnerReleaseInput.source_revision(%{})

    revision = String.duplicate("a", 40)
    assert {:ok, ^revision} = RunnerReleaseInput.source_revision(%{"source_revision" => revision})

    for invalid <- [123, "ABC", String.duplicate("a", 39), "\"\nRUN touch /injected"] do
      assert {:error, :invalid_runner_source_revision} =
               RunnerReleaseInput.source_revision(%{"source_revision" => invalid})
    end
  end

  test "build_runner/1 rejects an artifact root other than the current project", %{
    root_dir: root_dir
  } do
    assert {:error, {:unsupported_root_dir, _requested, _current}} =
             Dev.build_runner(
               root_dir: root_dir,
               skip_compile: true,
               allow_non_prod_build: true
             )
  end

  defp install!(root_dir) do
    assert {:ok, :installed} =
             Dev.install(
               root_dir: root_dir,
               docker_executable: "docker",
               docker_command_runner: &docker_runner/3,
               candidate_control_plane: %{
                 "reference" => "favn-control-plane-candidate:#{@control_build_id}",
                 "image_id" => @control_image_id
               }
             )
  end

  defp build(root_dir, opts \\ []) do
    Dev.build_runner(
      Keyword.merge(
        [
          root_dir: root_dir,
          skip_compile: true,
          skip_project_root_check: true,
          allow_non_prod_build: true,
          allow_unpinned_favn: true,
          host_toolchain: RunnerReleaseInput.expected_toolchain(),
          docker_executable: "docker",
          docker_command_runner: &docker_runner/3
        ],
        opts
      )
    )
  end

  defp dependency_sources do
    Mix.Dep.load_and_cache()
    |> Enum.reduce(%{}, fn dependency, sources ->
      case Keyword.get(dependency.opts, :dest) do
        path when is_binary(path) -> Map.put(sources, dependency.app, path)
        _missing -> sources
      end
    end)
  end

  defp dependency_input_digest(result) do
    result.dist_dir
    |> Path.join("dependency-input/dependency-input.json")
    |> File.read!()
    |> JSON.decode!()
    |> Map.fetch!("digest")
  end

  defp docker_runner("docker", args, _opts) do
    case args do
      ["version", "--format", "{{json .Server}}"] ->
        {JSON.encode!(%{"Os" => "linux", "Arch" => "amd64", "Version" => "28.3.0"}), 0}

      ["compose", "version", "--short"] ->
        {"2.39.1\n", 0}

      ["image", "inspect", _reference] ->
        {JSON.encode!([control_image_inspection()]), 0}

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

  defp control_image_inspection do
    %{
      "Id" => @control_image_id,
      "RepoDigests" => [],
      "Architecture" => "amd64",
      "Os" => "linux",
      "Config" => %{
        "User" => "10001:10001",
        "Labels" => %{
          "org.opencontainers.image.version" => RunnerRelease.current_favn_version(),
          "io.favn.control-plane.build-id" => @control_build_id,
          "io.favn.manifest-schema-version" =>
            Favn.Manifest.Compatibility.current_schema_version() |> Integer.to_string(),
          "io.favn.runner-contract-version" =>
            Favn.Manifest.Compatibility.current_runner_contract_version() |> Integer.to_string(),
          "io.favn.target" => "linux/amd64"
        }
      }
    }
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
