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
      "import Config\nconfig :tzdata, :autoupdate, :disabled\n"
    )

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
            {:favn, path: "../favn/apps/favn"}
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
    assert "lib/#{Macro.underscore(base_module(app))}/connections/warehouse.ex" in result.created
    assert "config/config.exs" in result.updated
    assert "mix.exs" in result.updated

    raw_orders =
      File.read!(
        Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/warehouse/raw/orders.ex")
      )

    assert raw_orders =~ "use Favn.Asset"
    assert raw_orders =~ "alias Favn.SQLClient"
    assert raw_orders =~ "ctx.asset.relation"
    assert raw_orders =~ ~S|create or replace table #{qualified_relation(relation)}|

    order_summary =
      File.read!(
        Path.join(
          root_dir,
          "lib/#{Macro.underscore(base_module(app))}/warehouse/gold/order_summary.ex"
        )
      )

    assert order_summary =~ "use Favn.SQLAsset"
    assert order_summary =~ "@materialized :table"
    assert order_summary =~ "from raw.orders"

    pipeline =
      File.read!(
        Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/pipelines/local_smoke.ex")
      )

    assert pipeline =~ "deps(:all)"

    config = File.read!(Path.join(root_dir, "config/config.exs"))
    assert config =~ "asset_modules: ["
    assert config =~ "connection_modules: ["
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
      Path.join(root_dir, "lib/#{Macro.underscore(base_module(app))}/warehouse/raw/orders.ex")

    File.write!(raw_path, "# local edit\n")

    assert {:ok, third} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)
    assert "lib/#{Macro.underscore(base_module(app))}/warehouse/raw/orders.ex" in third.skipped
    assert File.read!(raw_path) == "# local edit\n"
  end

  test "generated sample compiles into a manifest smoke path", %{root_dir: root_dir} do
    app = unique_app()
    base = base_module(app)
    lib_root = Path.join(root_dir, "lib/#{Macro.underscore(base)}")

    assert {:ok, _result} = Init.run(root_dir: root_dir, app: app, duckdb: true, sample: true)

    [
      "connections/warehouse.ex",
      "warehouse.ex",
      "warehouse/raw.ex",
      "warehouse/raw/orders.ex",
      "warehouse/gold.ex",
      "warehouse/gold/order_summary.ex",
      "pipelines/local_smoke.ex"
    ]
    |> Enum.each(fn relative ->
      Code.compile_file(Path.join(lib_root, relative))
    end)

    raw_orders = Module.concat([base, Warehouse, Raw, Orders])
    order_summary = Module.concat([base, Warehouse, Gold, OrderSummary])
    pipeline = Module.concat([base, Pipelines, LocalSmoke])

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [raw_orders, order_summary],
               pipeline_modules: [pipeline]
             )

    assert length(manifest.assets) == 2
    assert length(manifest.pipelines) == 1

    assert Enum.any?(manifest.graph.edges, fn edge ->
             edge.from == {raw_orders, :asset} and edge.to == {order_summary, :asset}
           end)
  end

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
          [app: :smoke_consumer, version: "0.1.0", elixir: "~> 1.19", deps: deps()]
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

          raw_ref = {#{base}.Warehouse.Raw.Orders, :asset}
          gold_ref = {#{base}.Warehouse.Gold.OrderSummary, :asset}

          for ref <- [raw_ref, gold_ref] do
            work = %Favn.Contracts.RunnerWork{
              run_id: "generated-" <> Atom.to_string(elem(ref, 1)),
              manifest_version_id: version.manifest_version_id,
              manifest_content_hash: version.content_hash,
              asset_ref: ref
            }

            assert {:ok, result} = FavnRunner.run(work, timeout: 30_000)
            assert result.status == :ok
          end

          assert {:ok, result} =
                   Favn.SQLClient.with_connection(:warehouse, [], fn session ->
                     Favn.SQLClient.query(
                       session,
                       "select order_date, order_count, revenue_cents from gold.order_summary order by order_date"
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

    assert output =~ "1 test, 0 failures"
  end

  test "requires explicit duckdb and sample flags", %{root_dir: root_dir} do
    assert {:error, {:missing_required_flags, [:duckdb, :sample]}} = Init.run(root_dir: root_dir)
  end

  defp unique_app do
    String.to_atom("sample_app_#{System.unique_integer([:positive])}")
  end

  defp base_module(app), do: app |> to_string() |> Macro.camelize()
end
