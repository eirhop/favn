defmodule Favn.Dev.Build.RunnerTest do
  use ExUnit.Case, async: false

  alias Favn.Dev
  alias Favn.Dev.Build.Artifact
  alias Favn.RunnerRelease

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

  test "build_runner/1 writes an immutable relocatable OCI context", %{root_dir: root_dir} do
    install!(root_dir)

    assert {:ok, result} = build(root_dir)
    assert result.status == :built
    assert result.build_id == result.runner_release_id
    assert Path.basename(result.dist_dir) == result.runner_release_id

    expected = [
      "Dockerfile",
      "bundle.json",
      "runner-release.json",
      "operator-notes.md",
      "manifest/bundle.json",
      "manifest/manifest-index.json",
      "release-input/mix.exs",
      "release-input/dependency-lock.json",
      "release-input/stamp_apps.exs",
      "release-input/apps/favn_local/mix.exs",
      "release-input/apps/favn_runner/priv/runner-release.json"
    ]

    assert Enum.all?(expected, &File.exists?(Path.join(result.dist_dir, &1)))
    assert File.dir?(Path.join(result.dist_dir, "manifest/execution-packages"))

    assert {:ok, descriptor_bytes} = File.read(result.descriptor_path)
    assert {:ok, descriptor} = RunnerRelease.decode(descriptor_bytes)
    assert descriptor.runner_release_id == result.runner_release_id
    assert Enum.any?(descriptor.runtime_applications, &(&1.application == "favn_local"))

    assert {:ok, bundle_bytes} = File.read(Path.join(result.dist_dir, "bundle.json"))
    assert {:ok, bundle} = JSON.decode(bundle_bytes)
    assert bundle["runner_release_id"] == result.runner_release_id
    assert bundle["kind"] == "favn_runner_build_context"
    assert Enum.any?(bundle["files"], &(&1["path"] == "manifest/bundle.json"))
    refute bundle_bytes =~ root_dir

    dockerfile = File.read!(Path.join(result.dist_dir, "Dockerfile"))
    assert dockerfile =~ "@sha256:"
    assert length(Regex.scan(~r/FROM --platform=linux\/amd64 /, dockerfile)) == 2
    assert dockerfile =~ "USER 10001:10001"
    assert dockerfile =~ ~s(ENTRYPOINT ["/opt/favn/bin/favn_runner"])
    refute dockerfile =~ root_dir

    for relative <- [
          "release-input/mix.exs",
          "release-input/config/config.exs",
          "release-input/stamp_apps.exs"
        ] do
      assert {:ok, _quoted} =
               result.dist_dir
               |> Path.join(relative)
               |> File.read!()
               |> Code.string_to_quoted(file: relative)
    end

    evaluated_config =
      result.dist_dir
      |> Path.join("release-input/config/config.exs")
      |> Config.Reader.read!()

    assert is_list(Keyword.get(evaluated_config, :favn))

    assert File.read!(Path.join(result.dist_dir, "release-input/mix.exs")) =~ "runtime: false"

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
               allow_unpinned_favn: true
             )

    assert manifest_result.required_runner_release_id == result.runner_release_id
    assert File.exists?(manifest_result.manifest_path)

    File.write!(manifest_result.manifest_path, "tampered")

    assert {:error, :artifact_bundle_invalid} =
             Artifact.verify_bundle(manifest_result.dist_dir, "favn_manifest_release")
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
               allow_unpinned_favn: true
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
    assert {:ok, result} = build(root_dir, lock: %{authoring_only: {:hex, :one}})

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true,
               lock: %{authoring_only: {:hex, :two}}
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

    refute File.read!(Path.join(result.dist_dir, "release-input/config/config.exs")) =~
             "import_config"

    assert File.read!(Path.join(result.dist_dir, "release-input/config/runtime.exs")) =~
             ~s(import_config "customer_config/runtime.exs")

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "release-input/rel/overlays/releases/1.0.0/customer_config/runtime.exs"
             )
           )

    refute File.exists?(
             Path.join(
               result.dist_dir,
               "release-input/rel/overlays/releases/1.0.0/customer_config/dev.exs"
             )
           )

    assert File.read!(
             Path.join(result.dist_dir, "release-input/apps/favn_local/priv/resource.txt")
           ) == "customer-resource"

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "release-input/apps/favn_local/priv/docs/schema.json"
             )
           )

    assert File.exists?(
             Path.join(
               result.dist_dir,
               "release-input/apps/favn_local/priv/test/fixture.bin"
             )
           )

    File.write!(Path.join(customer, "priv/resource.txt"), "changed-resource")

    assert {:error, {:runner_rebuild_required, categories}} =
             Dev.build_manifest(
               root_dir: root_dir,
               runner_release: result.descriptor_path,
               current_app_source: customer,
               skip_compile: true,
               allow_non_prod_build: true,
               allow_unpinned_favn: true
             )

    assert :runtime_dependencies in categories
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
               skip_tool_checks: true,
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
               allow_unpinned_favn: true
             )

    assert :plugins in categories
  end

  test "build_runner/1 requires install", %{root_dir: root_dir} do
    assert {:error, :install_required} = build(root_dir)
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
                 "set -- /opt/favn/lib/favn_local-*/ebin/Elixir.Favn.Dev.Build.Runner.beam; test ! -e \"$1\""
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
               allow_unpinned_favn: true
             )

    assert Path.wildcard(Path.join(root_dir, ".favn/dist/manifest/*")) == []
  end

  test "build_runner/1 rejects an artifact root other than the current project", %{
    root_dir: root_dir
  } do
    assert {:error, {:unsupported_root_dir, _requested, _current}} =
             Dev.build_runner(
               root_dir: root_dir,
               skip_compile: true,
               skip_tool_checks: true,
               allow_non_prod_build: true
             )
  end

  defp install!(root_dir) do
    assert {:ok, :installed} =
             Dev.install(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )
  end

  defp build(root_dir, opts \\ []) do
    Dev.build_runner(
      Keyword.merge(
        [
          root_dir: root_dir,
          skip_compile: true,
          skip_tool_checks: true,
          skip_project_root_check: true,
          allow_non_prod_build: true,
          allow_unpinned_favn: true
        ],
        opts
      )
    )
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
