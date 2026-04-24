defmodule Favn.Manifest.GeneratorTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest

  defmodule TestSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")
  end

  defmodule TestAsset do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
    use Favn.Asset

    @meta category: :sales, tags: [:raw]
    @relation true
    def asset(_ctx), do: :ok
  end

  defmodule TestSQLAsset do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "sales"]
    use Favn.SQLAsset

    @materialized :table
    query do
      ~SQL"SELECT 1 AS id"
    end
  end

  defmodule TestPipeline do
    use Favn.Pipeline

    pipeline :daily_sales do
      asset(TestAsset)
      deps(:all)
      schedule({TestSchedules, :daily})
    end
  end

  defmodule RelationOrders do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "orders"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomers do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "customers"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomer360 do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "commerce"]
    use Favn.SQLAsset

    @materialized :view
    query do
      ~SQL"""
      select o.id, c.id as customer_id
      from raw.commerce.orders o
      join raw.commerce.customers c on c.id = o.customer_id
      """
    end
  end

  test "generates manifest from explicit module lists" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(
               asset_modules: [TestAsset, TestSQLAsset],
               pipeline_modules: [TestPipeline],
               schedule_modules: [TestSchedules]
             )

    assert manifest.schema_version == 1
    assert manifest.runner_contract_version == 1
    assert length(manifest.assets) == 2
    assert length(manifest.pipelines) == 1
    assert length(manifest.schedules) == 1
    assert manifest.graph.nodes == [{TestAsset, :asset}, {TestSQLAsset, :asset}]

    assert Enum.any?(manifest.assets, &(&1.ref == {TestAsset, :asset}))

    sql_asset = Enum.find(manifest.assets, &(&1.ref == {TestSQLAsset, :asset}))
    assert sql_asset.type == :sql
    assert %Favn.Manifest.SQLExecution{} = sql_asset.sql_execution

    [pipeline] = manifest.pipelines
    assert pipeline.name == :daily_sales

    [schedule] = manifest.schedules
    assert schedule.module == TestSchedules
    assert schedule.name == :daily
  end

  test "lists and fetches compiled assets without registry" do
    assert {:ok, [asset]} = Favn.list_assets([TestAsset])
    assert asset.ref == {TestAsset, :asset}

    assert {:ok, fetched} = Favn.get_asset(TestAsset)
    assert fetched.ref == {TestAsset, :asset}
  end

  test "generates manifest dependencies from relation-style SQL references" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(
               asset_modules: [RelationOrders, RelationCustomers, RelationCustomer360]
             )

    downstream = Enum.find(manifest.assets, &(&1.ref == {RelationCustomer360, :asset}))

    assert downstream.depends_on == [
             {RelationCustomers, :asset},
             {RelationOrders, :asset}
           ]
  end

  test "generates manifest for same-batch SQL asset namespace inheritance" do
    root = Module.concat(__MODULE__, "ParallelSQLRoot#{System.unique_integer([:positive])}")
    gold = Module.concat(root, Gold)
    asset = Module.concat(gold, ExecutiveOverview)

    compile_modules_to_path!([
      {"sql_asset.ex",
       """
       defmodule #{inspect(asset)} do
         use Favn.Namespace
         use Favn.SQLAsset

         @materialized :view
         query do
           ~SQL\"\"\"
           select * from executive_overview
           \"\"\"
         end
       end
       """},
      {"root.ex",
       """
       defmodule #{inspect(root)} do
         Process.sleep(700)
         use Favn.Namespace, relation: [connection: :warehouse]
       end
       """},
      {"gold.ex",
       """
       defmodule #{inspect(gold)} do
         Process.sleep(700)
         use Favn.Namespace, relation: [schema: :gold]
       end
       """}
    ])

    assert {:ok, %Manifest{} = manifest} = Favn.generate_manifest(asset_modules: [asset])

    assert [manifest_asset] = manifest.assets
    assert manifest_asset.ref == {asset, :asset}
    assert manifest_asset.relation.connection == :warehouse
    assert manifest_asset.relation.schema == "gold"
    assert manifest_asset.relation.name == "executive_overview"
  end

  test "lists assets with inferred dependencies when given a module list" do
    assert {:ok, assets} =
             Favn.list_assets([RelationOrders, RelationCustomers, RelationCustomer360])

    downstream = Enum.find(assets, &(&1.ref == {RelationCustomer360, :asset}))

    assert downstream.depends_on == [
             {RelationCustomers, :asset},
             {RelationOrders, :asset}
           ]
  end

  test "builds, hashes, validates, and pins manifest versions" do
    assert {:ok, build} =
             Favn.build_manifest(
               asset_modules: [TestAsset],
               pipeline_modules: [TestPipeline],
               schedule_modules: [TestSchedules]
             )

    assert is_map(build.manifest)
    assert is_struct(build.manifest, Favn.Manifest)

    assert {:ok, _json} = Favn.serialize_manifest(build)
    assert {:ok, hash} = Favn.hash_manifest(build)
    assert byte_size(hash) == 64

    assert :ok =
             Favn.validate_manifest_compatibility(%{
               schema_version: 1,
               runner_contract_version: 1
             })

    assert {:ok, version} =
             Favn.pin_manifest_version(build,
               manifest_version_id: "mv_test_facade_001",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert version.manifest_version_id == "mv_test_facade_001"
    assert version.content_hash == hash
  end

  defp compile_modules_to_path!(entries) when is_list(entries) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_manifest_parallel_modules_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)

    files =
      Enum.map(entries, fn {name, source} ->
        file_path = Path.join(dir, name)
        File.write!(file_path, source)
        file_path
      end)

    Code.prepend_path(dir)

    assert {:ok, _modules, _diagnostics} =
             Kernel.ParallelCompiler.compile_to_path(files, dir, return_diagnostics: true)

    :ok
  end
end
