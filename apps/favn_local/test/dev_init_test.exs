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

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [raw_orders, order_summary],
               pipeline_modules: [pipeline],
               runner_release: FavnTestSupport.runner_release()
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

  test "requires explicit duckdb and sample flags", %{root_dir: root_dir} do
    assert {:error, {:missing_required_flags, [:duckdb, :sample]}} = Init.run(root_dir: root_dir)
  end

  test "scaffolds an idempotent consumer-owned local Compose template", %{root_dir: root_dir} do
    assert {:ok, first} = Init.run(root_dir: root_dir, target: :compose)

    assert first.profile == :local
    assert first.output == "deploy/compose.local.yml"
    assert first.env_example == "deploy/compose.local.env.example"

    assert Enum.sort(first.created) ==
             ["deploy/compose.local.env.example", "deploy/compose.local.yml"]

    compose = File.read!(Path.join(root_dir, first.output))
    assert compose =~ ~s(io.favn.compose.contract-version: "1")
    assert compose =~ "io.favn.compose.profile: local"
    assert compose =~ "io.favn.compose.role: runner"
    assert compose =~ "source: ${FAVN_RUNNER_DATA_SOURCE}"
    assert compose =~ "target: /var/lib/favn/data"
    assert compose =~ "user: ${FAVN_RUNNER_UID}:${FAVN_RUNNER_GID}"
    assert compose =~ ~s(env_file: ["${FAVN_RUNNER_ENV_FILE}"])

    assert compose =~
             "../.favn/compose/postgres-init.sh:/docker-entrypoint-initdb.d/10-favn-runtime-role.sh:ro"

    assert {:ok, second} = Init.run(root_dir: root_dir, target: :compose)
    assert second.created == []
    assert Enum.sort(second.existing) == Enum.sort(first.created)
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
    assert result.output == "deploy/compose.single-host.yml"
    assert compose =~ "io.favn.compose.profile: single-host"
    refute compose =~ "io.favn.compose.role: postgres"
    assert compose =~ "FAVN_POSTGRES_ADMIN_DATABASE_URL"
    assert compose =~ "FAVN_POSTGRES_RUNTIME_DATABASE_URL"
    refute compose =~ "internal: true"
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
