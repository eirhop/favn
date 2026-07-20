defmodule Favn.Manifest.GeneratorTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest

  defmodule TestSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")
  end

  defmodule TestAsset do
    use Favn.Asset

    meta(category: :sales, tags: [:raw])
    relation(connection: :warehouse, catalog: "raw", schema: "sales")
    def asset(_ctx), do: :ok
  end

  defmodule TestSQLAsset do
    use Favn.SQLAsset

    materialized(:table)
    relation(connection: :warehouse, catalog: "gold", schema: "sales")

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

  defmodule RelationRaw do
    use Favn.Namespace

    relation(connection: :warehouse, catalog: "raw", schema: "commerce")
  end

  defmodule RelationRaw.Orders do
    use Favn.Asset

    relation(true)
    def asset(_ctx), do: :ok
  end

  defmodule RelationRaw.Customers do
    use Favn.Asset

    relation(true)
    def asset(_ctx), do: :ok
  end

  defmodule RelationGold do
    use Favn.Namespace

    relation(connection: :warehouse, catalog: "gold", schema: "commerce")
  end

  defmodule RelationGold.Customer360 do
    use Favn.SQLAsset

    materialized(:view)
    relation(true)

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

    assert manifest.schema_version == 9
    assert manifest.runner_contract_version == 9
    assert length(manifest.assets) == 2
    assert length(manifest.pipelines) == 1
    assert length(manifest.schedules) == 1
    assert manifest.graph.nodes == [{TestAsset, :asset}, {TestSQLAsset, :asset}]

    assert Enum.any?(manifest.assets, &(&1.ref == {TestAsset, :asset}))

    sql_asset = Enum.find(manifest.assets, &(&1.ref == {TestSQLAsset, :asset}))
    assert sql_asset.type == :sql
    assert sql_asset.execution_package_hash =~ ~r/^[0-9a-f]{64}$/

    assert {:ok, build} =
             Favn.build_manifest(
               asset_modules: [TestAsset, TestSQLAsset],
               pipeline_modules: [TestPipeline],
               schedule_modules: [TestSchedules]
             )

    assert {:ok, publication} = Favn.prepare_manifest_publication(build)
    assert [package] = publication.execution_packages
    assert package.content_hash == sql_asset.execution_package_hash
    assert %Favn.Manifest.SQLExecution{} = package.sql_execution

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
               asset_modules: [
                 RelationRaw.Orders,
                 RelationRaw.Customers,
                 RelationGold.Customer360
               ]
             )

    downstream = Enum.find(manifest.assets, &(&1.ref == {RelationGold.Customer360, :asset}))

    assert downstream.depends_on == [
             {RelationRaw.Customers, :asset},
             {RelationRaw.Orders, :asset}
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
         use Favn.SQLAsset

         materialized :view
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
         Process.sleep(100)
         use Favn.Namespace
         relation connection: :warehouse
       end
       """},
      {"gold.ex",
       """
       defmodule #{inspect(gold)} do
         Process.sleep(100)
         use Favn.Namespace
         relation schema: :gold
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
             Favn.list_assets([
               RelationRaw.Orders,
               RelationRaw.Customers,
               RelationGold.Customer360
             ])

    downstream = Enum.find(assets, &(&1.ref == {RelationGold.Customer360, :asset}))

    assert downstream.depends_on == [
             {RelationRaw.Customers, :asset},
             {RelationRaw.Orders, :asset}
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
               schema_version: 8,
               runner_contract_version: 8
             })

    assert {:ok, version} =
             Favn.pin_manifest_version(build,
               manifest_version_id: "mv_test_facade_001",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert version.manifest_version_id == "mv_test_facade_001"
    assert version.content_hash == hash
  end

  test "namespace changes affect descendant manifest identity but not unrelated assets" do
    suffix = System.unique_integer([:positive])
    root = Module.concat(__MODULE__, "FingerprintNamespace#{suffix}")
    descendant = Module.concat(root, Orders)
    unrelated = Module.concat(__MODULE__, "FingerprintUnrelated#{suffix}")

    Code.compile_string("""
    defmodule #{inspect(descendant)} do
      use Favn.Asset
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(unrelated)} do
      use Favn.Asset
      settings stable: true
      def asset(_ctx), do: :ok
    end
    """)

    compile_namespace = fn owner ->
      :code.purge(root)
      :code.delete(root)

      Code.compile_string("""
      defmodule #{inspect(root)} do
        use Favn.Namespace
        settings namespace_revision: #{inspect(owner)}
        meta owner: #{inspect(owner)}
      end
      """)
    end

    compile_namespace.("platform-v1")
    assert {:ok, first_descendant} = Favn.generate_manifest(asset_modules: [descendant])
    assert {:ok, first_unrelated} = Favn.generate_manifest(asset_modules: [unrelated])

    compile_namespace.("platform-v2")
    assert {:ok, second_descendant} = Favn.generate_manifest(asset_modules: [descendant])
    assert {:ok, second_unrelated} = Favn.generate_manifest(asset_modules: [unrelated])

    assert {:ok, first_descendant_hash} = Favn.hash_manifest(first_descendant)
    assert {:ok, second_descendant_hash} = Favn.hash_manifest(second_descendant)
    assert {:ok, first_unrelated_hash} = Favn.hash_manifest(first_unrelated)
    assert {:ok, second_unrelated_hash} = Favn.hash_manifest(second_unrelated)
    assert first_descendant_hash != second_descendant_hash
    assert first_unrelated_hash == second_unrelated_hash

    assert [first_asset] = first_descendant.assets
    assert [second_asset] = second_descendant.assets
    assert first_asset.metadata.owner == "platform-v1"
    assert second_asset.metadata.owner == "platform-v2"
  end

  defp compile_modules_to_path!(entries) when is_list(entries) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "favn_manifest_parallel_modules_#{Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)}"
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
