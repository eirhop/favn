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
        Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/lakehouse/raw/sales/orders.ex")
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
    assert config =~ "raw_catalog: [file: {:priv, #{inspect(app)}, \"duckdb/raw_catalog.sql\"}]"
    assert config =~ "catalogs: ["
    assert config =~ "raw: [resource: :raw_catalog, write_concurrency: 1]"
    assert File.read!(Path.join(root_dir, "priv/duckdb/raw_catalog.sql")) =~
             "ATTACH '.favn/data/raw.duckdb' AS raw"
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
      Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/lakehouse/raw/sales/orders.ex")

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
               pipeline_modules: [pipeline]
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

  @tag :slow
  @tag timeout: 180_000
  test "generated sample executes raw and SQL assets against DuckDB", %{root_dir: root_dir} do
    repo_root = Path.expand("../../..", __DIR__)
    app = :smoke_consumer
    base = base_module(app)

    File.write!(
      Path.join(root_dir, "mix.exs"),
      """
      defmodule SmokeConsumer.MixProject do
        use Mix.Project

        def project do
          [app: :smoke_consumer, version: "0.1.0", elixir: "~> 1.20", deps: deps()]
        end

        def application do
          [extra_applications: [:logger]]
        end

        defp deps do
          [
            {:favn, path: #{inspect(Path.join(repo_root, "apps/favn"))}}
          ]
        end
      end
      """
    )

    assert {:ok, _result} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)
    File.mkdir_p!(Path.join(root_dir, "test"))
    File.write!(Path.join(root_dir, "test/test_helper.exs"), "ExUnit.start()\n")

    File.write!(
      Path.join(root_dir, "test/generated_smoke_test.exs"),
      """
      defmodule GeneratedSmokeTest do
        use ExUnit.Case, async: false

        test "generated Favn sample runs against DuckDB" do
          File.mkdir_p!(".favn/data")
          {:ok, _started} = Application.ensure_all_started(:favn_runner)

          {:ok, manifest} = Favn.generate_manifest()
          {:ok, version} = Favn.pin_manifest_version(manifest)
          :ok = FavnRunner.register_manifest(version)

          raw_ref = {#{base}.Lakehouse.Raw.Sales.Orders, :asset}
          mart_ref = {#{base}.Lakehouse.Mart.Sales.OrderSummary, :asset}

          for ref <- [raw_ref, mart_ref] do
            run_id =
              ref
              |> elem(0)
              |> Atom.to_string()
              |> String.replace(".", "_")

            work = %Favn.Contracts.RunnerWork{
              run_id: "generated-" <> run_id,
              manifest_version_id: version.manifest_version_id,
              manifest_content_hash: version.content_hash,
              asset_ref: ref
            }

            assert {:ok, result} = FavnRunner.run(work, timeout: 30_000)
            assert result.status == :ok, inspect(result)
          end

          assert {:ok, result} =
                   Favn.SQLClient.with_connection(:important_lakehouse, [], fn session ->
                      Favn.SQLClient.query(
                        session,
                        "select order_date, order_count, revenue_cents from mart.sales.order_summary order by order_date"
                      )
                   end)

          assert [first, second] = result.rows
          assert value(first["order_count"]) == 2
          assert value(first["revenue_cents"]) == 20500
          assert value(second["order_count"]) == 1
          assert value(second["revenue_cents"]) == 1575
        end

        defp value({_scale, integer}), do: integer
        defp value(value), do: value
      end
      """
    )

    assert {_output, 0} = System.cmd("mix", ["deps.get"], cd: root_dir, stderr_to_stdout: true)

    assert {output, 0} =
             System.cmd("mix", ["test", "test/generated_smoke_test.exs"],
               cd: root_dir,
               stderr_to_stdout: true
             )

    assert output =~ "Result: 1 passed"
  end

  test "requires explicit duckdb and sample flags", %{root_dir: root_dir} do
    assert {:error, {:missing_required_flags, [:duckdb, :sample]}} = Init.run(root_dir: root_dir)
  end

  defp unique_app do
    String.to_atom("sample_app_#{System.unique_integer([:positive])}")
  end

  defp base_module(app), do: app |> to_string() |> Macro.camelize()
end
