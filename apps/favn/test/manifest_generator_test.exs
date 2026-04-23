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
end
