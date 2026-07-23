defmodule Favn.Dev.InitTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Init

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_init_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    File.mkdir_p!(Path.join(root_dir, "config"))

    File.write!(
      Path.join(root_dir, "config/config.exs"),
      "import Config\n"
    )

    favn_path = Path.expand("../../favn", __DIR__)

    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule SampleApp.MixProject do
        use Mix.Project

        def project do
          [app: :sample_app, version: "0.1.0", deps: deps()]
        end

        def application do
          []
        end

        defp deps do
          [
            {:favn, path: #{inspect(favn_path)}}
          ]
        end
      end
      """
    )

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "generates explicit DuckDB sample files and config", %{root_dir: root_dir} do
    app = unique_app()

    assert {:ok, result} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)

    assert result.pipeline_module == "#{base_module(app)}.Pipelines.LocalSmoke"

    assert "lib/#{Macro.underscore(base_module(app))}/connections/important_lakehouse.ex" in result.created

    assert "config/config.exs" in result.updated
    assert "mix.exs" in result.updated
    assert "priv/duckdb/raw_catalog.sql" in result.created
    assert "priv/duckdb/mart_catalog.sql" in result.created

    raw_orders =
      File.read!(
        Path.join(
          root_dir,
          "lib/#{Macro.underscore(base_module(app))}/lakehouse/raw/sales/orders.ex"
        )
      )

    assert raw_orders =~ "use Favn.Asset"
    assert raw_orders =~ "@compile {:no_warn_undefined, Favn.SQLClient}"
    assert raw_orders =~ "alias Favn.SQLClient"
    assert raw_orders =~ "ctx.asset.relation"
    assert raw_orders =~ ~S|create or replace table #{qualified_relation(relation)}|

    order_summary =
      File.read!(
        Path.join(
          root_dir,
          "lib/#{Macro.underscore(base_module(app))}/lakehouse/mart/sales/order_summary.ex"
        )
      )

    assert order_summary =~ "use Favn.SQLAsset"
    assert order_summary =~ "depends #{base_module(app)}.Lakehouse.Raw.Sales.Orders"
    assert order_summary =~ "materialized :table"
    assert order_summary =~ "relation true"
    assert order_summary =~ "from #{base_module(app)}.Lakehouse.Raw.Sales.Orders"

    pipeline =
      File.read!(
        Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/pipelines/local_smoke.ex")
      )

    assert pipeline =~ "deps(:all)"

    config = File.read!(Path.join(root_dir, "config/config.exs"))
    assert config =~ "discovery: ["
    assert config =~ "apps: [#{inspect(app)}]"
    assert config =~ "assets: :all"
    assert config =~ "pipelines: :all"
    assert config =~ "schedules: :all"
    assert config =~ "connections: :all"
    assert config =~ "important_lakehouse: ["
    assert config =~ "resources: ["
    assert config =~ "FAVN_LOCAL_SAMPLE_DATABASE_PATH"
    assert config =~ "FAVN_LOCAL_SAMPLE_RAW_CATALOG_PATH"
    assert config =~ "FAVN_LOCAL_SAMPLE_MART_CATALOG_PATH"
    assert config =~ "raw_catalog: ["
    assert config =~ "file: {:priv, #{inspect(app)}, \"duckdb/raw_catalog.sql\"}"
    assert config =~ "params: ["
    assert config =~ "catalogs: ["
    assert config =~ "raw: [resource: :raw_catalog, write_concurrency: 1]"

    assert File.read!(Path.join(root_dir, "priv/duckdb/raw_catalog.sql")) =~
             "ATTACH @database_path AS raw"

    assert config =~ "runner_plugins: ["
    assert config =~ "FavnDuckdb"

    env_example = File.read!(Path.join(root_dir, ".env.example"))
    refute env_example =~ "FAVN_ORCHESTRATOR_BOOTSTRAP"
  end

  test "is idempotent and leaves changed files untouched", %{root_dir: root_dir} do
    app = unique_app()

    assert {:ok, first} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)
    assert first.created != []

    assert {:ok, second} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)
    assert second.created == []
    assert "config/config.exs" in second.existing
    assert "mix.exs" in second.existing

    raw_path =
      Path.join(
        root_dir,
        "lib/#{Macro.underscore(base_module(app))}/lakehouse/raw/sales/orders.ex"
      )

    File.write!(raw_path, "# local edit\n")

    assert {:ok, third} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)

    assert "lib/#{Macro.underscore(base_module(app))}/lakehouse/raw/sales/orders.ex" in third.skipped

    assert File.read!(raw_path) == "# local edit\n"
  end

  test "generated sample compiles into a manifest smoke path", %{root_dir: root_dir} do
    app = unique_app()
    base = base_module(app)
    lib_root = Path.join(root_dir, "lib/#{Macro.underscore(base)}")

    assert {:ok, _result} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)

    [
      "connections/important_lakehouse.ex",
      "lakehouse.ex",
      "lakehouse/raw.ex",
      "lakehouse/raw/sales.ex",
      "lakehouse/raw/sales/orders.ex",
      "lakehouse/mart.ex",
      "lakehouse/mart/sales.ex",
      "lakehouse/mart/sales/order_summary.ex",
      "pipelines/local_smoke.ex"
    ]
    |> Enum.each(fn relative ->
      Code.compile_file(Path.join(lib_root, relative))
    end)

    raw_orders = Module.concat([base, Lakehouse, Raw, Sales, Orders])
    order_summary = Module.concat([base, Lakehouse, Mart, Sales, OrderSummary])
    pipeline = Module.concat([base, Pipelines, LocalSmoke])
    connection = Module.concat([base, Connections, ImportantLakehouse])

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [raw_orders, order_summary],
               pipeline_modules: [pipeline],
               connection_modules: [connection],
               runner_release_id: FavnTestSupport.runner_release_id()
             )

    assert length(manifest.assets) == 2
    assert length(manifest.pipelines) == 1

    assert raw = Enum.find(manifest.assets, &(&1.ref == {raw_orders, :asset}))
    assert raw.relation.connection == :important_lakehouse
    assert raw.relation.catalog == "raw"
    assert raw.relation.schema == "sales"

    assert mart = Enum.find(manifest.assets, &(&1.ref == {order_summary, :asset}))
    assert mart.relation.connection == :important_lakehouse
    assert mart.relation.catalog == "mart"
    assert mart.relation.schema == "sales"

    assert Enum.any?(manifest.graph.edges, fn edge ->
             edge.from == {raw_orders, :asset} and edge.to == {order_summary, :asset}
           end)
  end

  test "scaffolds the complete local project by default", %{root_dir: root_dir} do
    assert {:ok, result} = Init.run(root_dir: root_dir, app: :sample_app)

    assert result.target == :project
    assert result.compose.output == "deploy/local/compose.yml"
    assert result.runner.output == "deploy/runner"

    assert File.regular?(Path.join(root_dir, "deploy/local/compose.yml"))
    assert File.regular?(Path.join(root_dir, "deploy/runner/Dockerfile"))
  end

  test "scaffolds an idempotent consumer-owned local Compose template", %{root_dir: root_dir} do
    assert {:ok, first} = Init.run(root_dir: root_dir, target: :compose)

    assert first.profile == :local
    assert first.output == "deploy/local/compose.yml"
    assert first.env_example == "deploy/local/compose.env.example"

    assert Enum.sort(first.created) ==
             ["deploy/local/compose.env.example", "deploy/local/compose.yml"]

    compose = File.read!(Path.join(root_dir, first.output))
    assert compose =~ ~s(io.favn.compose.contract-version: "1")
    assert compose =~ "io.favn.compose.profile: local"
    assert compose =~ "io.favn.compose.role: runner"
    assert compose =~ "image: postgres:18-trixie"
    assert compose =~ "pull_policy: always"
    assert compose =~ "x-local-logging: &local-logging"
    assert compose =~ ~s(driver: local)
    assert compose =~ ~s(max-size: "10m")
    assert compose =~ ~s(max-file: "3")
    assert length(Regex.scan(~r/^\s+logging: \*local-logging$/m, compose)) == 5
    assert compose =~ "POSTGRES_INITDB_ARGS: >-"
    assert compose =~ "--encoding=UTF8"
    assert compose =~ "--locale-provider=builtin"
    assert compose =~ "--builtin-locale=C.UTF-8"
    assert compose =~ "source: ../../.data"
    assert compose =~ "target: /var/lib/favn/data"
    assert compose =~ "user: ${FAVN_RUNNER_UID}:${FAVN_RUNNER_GID}"
    assert compose =~ ~s(env_file: ["${FAVN_RUNNER_ENV_FILE}"])
    assert compose =~ "FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}"
    assert length(Regex.scan(~r/FAVN_LOG_LEVEL: \$\{FAVN_LOG_LEVEL:-info\}/, compose)) == 3

    assert compose =~
             "../../.favn/compose/postgres-init.sh:/docker-entrypoint-initdb.d/10-favn-runtime-role.sh:ro"

    assert compose =~ "This file belongs to the customer project"
    assert compose =~ "steady-state three-container topology"
    refute compose =~ "postgres@sha256:"

    assert {:ok, second} = Init.run(root_dir: root_dir, target: :compose)
    assert second.created == []
    assert Enum.sort(second.existing) == Enum.sort(first.created)
  end

  test "scaffolds an editable customer-owned runner image", %{root_dir: root_dir} do
    assert {:ok, first} =
             Init.run(root_dir: root_dir, app: :sample_app, target: :runner)

    assert first.target == :runner
    assert first.output == "deploy/runner"
    assert first.includes == []

    assert Enum.sort(first.created) ==
             [
               "deploy/runner/Dockerfile",
               "deploy/runner/Dockerfile.dockerignore",
               "deploy/runner/env.sh.eex",
               "deploy/runner/mix.exs"
             ]

    dockerfile = File.read!(Path.join(root_dir, "deploy/runner/Dockerfile"))
    release_project = File.read!(Path.join(root_dir, "deploy/runner/mix.exs"))

    assert dockerfile =~ "ARG FAVN_RUNNER_RELEASE_ID"
    assert dockerfile =~ "mix release favn_runner"
    refute dockerfile =~ "rel/overlays/releases"
    refute dockerfile =~ "cp -R ../../config/runtime"
    assert dockerfile =~ ~s(io.favn.runner-release-id="$FAVN_RUNNER_RELEASE_ID")
    assert dockerfile =~ "FROM --platform=linux/amd64 debian:trixie-slim AS runtime"
    refute dockerfile =~ "@sha256:"
    refute dockerfile =~ "DUCKDB_ADBC_DRIVER"
    assert release_project =~ "{:sample_app, path: \"../..\"}"
    assert release_project =~ "{:sample_app, :load}"
    refute release_project =~ "extra_applications: [:sample_app]"

    assert {:ok, second} =
             Init.run(root_dir: root_dir, app: :sample_app, target: :runner)

    assert second.created == []
    assert Enum.sort(second.existing) == Enum.sort(first.created)
  end

  test "packages only nonignored runtime configuration companions", %{root_dir: root_dir} do
    runtime_dir = Path.join(root_dir, "config/runtime")
    File.mkdir_p!(runtime_dir)
    File.write!(Path.join(runtime_dir, "local.exs"), "import Config\n")
    File.write!(Path.join(runtime_dir, "ignored.exs"), "raise \"must not be packaged\"\n")
    File.write!(Path.join(root_dir, ".gitignore"), "config/runtime/ignored.exs\n")

    assert {_output, 0} = System.cmd("git", ["init", "--quiet", root_dir])
    assert {:ok, _result} = Init.run(root_dir: root_dir, app: :sample_app, target: :runner)

    dockerfile = File.read!(Path.join(root_dir, "deploy/runner/Dockerfile"))

    assert dockerfile =~
             "install -D -m 0644 '../../config/runtime/local.exs' 'overlays/releases/1.0.0/runtime/local.exs'"

    refute dockerfile =~ "ignored.exs"
    refute dockerfile =~ "cp -R ../../config/runtime"
  end

  test "adds one supported DuckDB ADBC native driver on explicit include", %{
    root_dir: root_dir
  } do
    assert {:ok, result} =
             Init.run(
               root_dir: root_dir,
               app: :sample_app,
               target: :runner,
               include: "duckdb-adbc@1.5.4",
               project_dependencies: [
                 {:favn, path: "deps/favn"},
                 {:favn_duckdb_adbc, path: "deps/favn_duckdb_adbc"}
               ]
             )

    assert result.includes == ["duckdb-adbc@1.5.4"]

    dockerfile = File.read!(Path.join(root_dir, "deploy/runner/Dockerfile"))

    assert dockerfile =~ "AS duckdb-adbc-driver"
    assert dockerfile =~ "releases/download/v1.5.4/libduckdb-linux-amd64.zip"

    assert dockerfile =~
             "ADD --checksum=sha256:838d98a85e697bab9935010c88a8c67d3312ccedcab4cb4a0ba01da65113bb70"

    assert dockerfile =~
             ~s(driver: "/opt/duckdb/1.5.4/libduckdb.so")
  end

  test "validates runner include versions and declared dependencies", %{root_dir: root_dir} do
    assert {:error, {:runner_include_dependency_missing, "duckdb-adbc", :favn_duckdb_adbc}} =
             Init.run(
               root_dir: root_dir,
               app: :sample_app,
               target: :runner,
               include: "duckdb-adbc"
             )

    assert {:error, {:unsupported_runner_include_version, "duckdb-adbc", "1.6.0", ["1.5.4"]}} =
             Init.run(
               root_dir: root_dir,
               app: :sample_app,
               target: :runner,
               include: "duckdb-adbc@1.6.0",
               project_dependencies: [{:favn_duckdb_adbc, path: "deps/favn_duckdb_adbc"}]
             )
  end

  test "runner scaffold validates distribution inputs before constructing VM flags", %{
    root_dir: root_dir
  } do
    assert {:ok, _result} =
             Init.run(root_dir: root_dir, app: :sample_app, target: :runner)

    script = Path.join(root_dir, "deploy/runner/env.sh.eex")

    valid = [
      {"FAVN_RUNNER_NODE", "favn_runner@runner.favn.internal"},
      {"FAVN_DISTRIBUTION_COOKIE", "0123456789abcdefghijklmnopqrstuvwxyzABCD"},
      {"FAVN_BEAM_DISTRIBUTION_PORT", "9100"},
      {"ERL_EPMD_PORT", "4369"}
    ]

    assert {_output, 0} = System.cmd("sh", [script], env: valid, stderr_to_stdout: true)

    assert {logger_flags, 0} =
             System.cmd(
               "sh",
               ["-c", ~s(. "$1"; printf '%s' "$ERL_AFLAGS"), "sh", script],
               env: valid,
               stderr_to_stdout: true
             )

    assert logger_flags =~ "-kernel logger_level info"

    assert {debug_config, 0} =
             System.cmd(
               "sh",
               ["-c", ~s(. "$1"; printf '%s|%s' "$FAVN_LOG_LEVEL" "$ERL_AFLAGS"), "sh", script],
               env: [{"FAVN_LOG_LEVEL", "debug"} | valid],
               stderr_to_stdout: true
             )

    assert debug_config =~ "debug|"
    assert debug_config =~ "-kernel logger_level debug"

    for invalid_level <- ["verbose", "debug -s init stop"] do
      assert {log_output, log_status} =
               System.cmd("sh", [script],
                 env: [{"FAVN_LOG_LEVEL", invalid_level} | valid],
                 stderr_to_stdout: true
               )

      assert log_status != 0
      assert log_output =~ "invalid FAVN_LOG_LEVEL"
    end

    assert {node_output, node_status} =
             System.cmd("sh", [script],
               env:
                 List.keystore(
                   valid,
                   "FAVN_RUNNER_NODE",
                   0,
                   {"FAVN_RUNNER_NODE", "runner@localhost"}
                 ),
               stderr_to_stdout: true
             )

    assert node_status != 0
    assert node_output =~ "invalid FAVN_RUNNER_NODE"

    assert {port_output, port_status} =
             System.cmd("sh", [script],
               env:
                 List.keystore(
                   valid,
                   "FAVN_BEAM_DISTRIBUTION_PORT",
                   0,
                   {"FAVN_BEAM_DISTRIBUTION_PORT", "9100 -s init stop"}
                 ),
               stderr_to_stdout: true
             )

    assert port_status != 0
    assert port_output =~ "invalid FAVN_BEAM_DISTRIBUTION_PORT"
  end

  test "renders every project-relative path for a comparison output", %{root_dir: root_dir} do
    runtime_dir = Path.join(root_dir, "config/runtime")
    File.mkdir_p!(runtime_dir)
    File.write!(Path.join(runtime_dir, "local.exs"), "import Config\n")

    assert {:ok, result} =
             Init.run(
               root_dir: root_dir,
               app: :sample_app,
               target: :runner,
               output: "ops/images/favn-runner-next"
             )

    assert result.output == "ops/images/favn-runner-next"

    mix_project = File.read!(Path.join(root_dir, "#{result.output}/mix.exs"))
    dockerfile = File.read!(Path.join(root_dir, "#{result.output}/Dockerfile"))

    assert mix_project =~ ~s(config_path: "../../../config/config.exs")
    assert mix_project =~ ~s(lockfile: "../../../mix.lock")
    assert mix_project =~ ~s({:sample_app, path: "../../.."})

    assert dockerfile =~
             "WORKDIR /build/${FAVN_PROJECT_ROOT}/ops/images/favn-runner-next"

    assert dockerfile =~
             "install -D -m 0644 '../../../config/runtime/local.exs' 'overlays/releases/1.0.0/runtime/local.exs'"
  end

  test "rejects a runner output directory symlink", %{root_dir: root_dir} do
    outside = Path.join(System.tmp_dir!(), "favn_runner_output_#{System.unique_integer()}")
    output = Path.join(root_dir, "deploy/runner")
    File.mkdir_p!(outside)
    File.mkdir_p!(Path.dirname(output))
    File.ln_s!(outside, output)
    on_exit(fn -> File.rm_rf(outside) end)

    assert {:error, {:unsafe_runner_output, "deploy/runner"}} =
             Init.run(root_dir: root_dir, app: :sample_app, target: :runner)

    assert File.ls!(outside) == []
  end

  test "refuses a modified scaffold without partially writing its companion", %{
    root_dir: root_dir
  } do
    output = Path.join(root_dir, "deploy/compose.team.yml")
    env_example = Path.rootname(output) <> ".env.example"
    File.mkdir_p!(Path.dirname(output))
    File.write!(output, "# team owned\n")

    assert {:error, {:compose_scaffold_modified, ^output}} =
             Init.run(root_dir: root_dir, target: :compose, output: "deploy/compose.team.yml")

    assert File.read!(output) == "# team owned\n"
    refute File.exists?(env_example)
  end

  test "never follows or overwrites the former predictable temporary path", %{
    root_dir: root_dir
  } do
    output = Path.join(root_dir, "deploy/compose.safe.yml")
    victim = Path.join(root_dir, "consumer-owned.txt")
    predictable_temporary = output <> ".favn-new"
    File.mkdir_p!(Path.dirname(output))
    File.write!(victim, "keep me\n")
    File.ln_s!(victim, predictable_temporary)

    assert {:ok, result} =
             Init.run(root_dir: root_dir, target: :compose, output: "deploy/compose.safe.yml")

    assert result.output == "deploy/compose.safe.yml"
    assert File.read!(victim) == "keep me\n"
    assert File.lstat!(predictable_temporary).type == :symlink
  end

  test "scaffolds the external PostgreSQL single-host reference", %{root_dir: root_dir} do
    assert {:ok, result} =
             Init.run(root_dir: root_dir, target: :compose, profile: :single_host)

    compose = File.read!(Path.join(root_dir, result.output))
    assert result.output == "deploy/single-host/compose.yml"
    assert compose =~ "io.favn.compose.profile: single-host"
    refute compose =~ "io.favn.compose.role: postgres"
    assert compose =~ "FAVN_POSTGRES_ADMIN_DATABASE_URL"
    assert compose =~ "FAVN_POSTGRES_RUNTIME_DATABASE_URL"
    assert compose =~ "FAVN_LOG_LEVEL: ${FAVN_LOG_LEVEL:-info}"
    refute compose =~ "internal: true"
    refute compose =~ "x-local-logging"
    refute compose =~ "logging: *local-logging"
  end

  @tag :container
  test "single-host reference renders through Docker Compose with every profile", %{
    root_dir: root_dir
  } do
    assert {:ok, result} =
             Init.run(root_dir: root_dir, target: :compose, profile: :single_host)

    compose_file = Path.join(root_dir, result.output)
    env_file = Path.join(root_dir, result.env_example)

    assert {rendered, 0} =
             System.cmd(
               "docker",
               [
                 "compose",
                 "--file",
                 compose_file,
                 "--env-file",
                 env_file,
                 "--profile",
                 "*",
                 "config",
                 "--format",
                 "json"
               ],
               stderr_to_stdout: true
             )

    assert {:ok, %{"services" => services}} = JSON.decode(rendered)

    assert Enum.sort(Map.keys(services)) ==
             ["control-plane", "control-plane-ops", "control-plane-verify", "runner"]
  end

  test "supports a fresh alternate output inside the project", %{root_dir: root_dir} do
    assert {:ok, result} =
             Init.run(
               root_dir: root_dir,
               target: :compose,
               output: "ops/team/compose.custom.yaml"
             )

    assert result.output == "ops/team/compose.custom.yaml"
    assert File.regular?(Path.join(root_dir, result.output))
    assert File.regular?(Path.join(root_dir, "ops/team/compose.custom.env.example"))
  end

  defp unique_app do
    String.to_atom("sample_app_#{System.unique_integer([:positive])}")
  end

  defp base_module(app), do: app |> to_string() |> Macro.camelize()
end
