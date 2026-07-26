defmodule FavnReferenceWorkload.ManifestGenerationTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias FavnReferenceWorkload.CRMData

  @runner_release_id "rr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

  test "CRM workload compiles into one layered manifest" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(runner_release_id: @runner_release_id)

    assert length(manifest.assets) == 18
    assert length(manifest.pipelines) == 3

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref ==
               {FavnReferenceWorkload.Warehouse.Landing.GenerateSeed, :asset} and
               asset.type == :elixir
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref ==
               {FavnReferenceWorkload.Warehouse.Landing.WriteExtracts, :deals_daily}
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref == {FavnReferenceWorkload.Warehouse.Mart.ExecutiveOverview, :asset}
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref == {FavnReferenceWorkload.Lifecycle.RetryProbe, :asset} and
               asset.retry_policy.max_attempts == 3
           end)

    assert manifest.assets
           |> Enum.filter(&(&1.type == :sql))
           |> Enum.all?(&(&1.execution_pool == :local_duckdb))

    assert Enum.any?(manifest.pipelines, &(&1.name == :bootstrap_crm_demo))
    assert Enum.any?(manifest.pipelines, &(&1.name == :daily_crm_analytics))

    assert Enum.any?(manifest.pipelines, fn pipeline ->
             pipeline.name == :lifecycle_schedule_probe and
               match?({:inline, %{cron: "*/10 * * * * *"}}, pipeline.schedule)
           end)
  end

  test "manifest content hash is stable across JSON publication boundary" do
    assert {:ok, build} = FavnAuthoring.build_manifest(runner_release_id: @runner_release_id)
    assert {:ok, original} = FavnAuthoring.pin_manifest_version(build.manifest)
    assert {:ok, public_hash} = FavnAuthoring.hash_manifest(build.manifest)
    assert public_hash == original.content_hash

    decoded =
      build.manifest
      |> Favn.Manifest.Serializer.encode_manifest!()
      |> JSON.decode!()

    assert {:ok, verified} =
             Favn.Manifest.Version.from_published(decoded,
               manifest_version_id: original.manifest_version_id,
               content_hash: original.content_hash,
               schema_version: original.schema_version,
               runner_contract_version: original.runner_contract_version,
               required_runner_release_id: original.required_runner_release_id,
               serialization_format: original.serialization_format
             )

    assert verified.content_hash == original.content_hash
  end

  test "account schema fixture compiles as distinct V1 and V2 contracts" do
    v1 = compile_account_variant!(:v1)
    v2 = compile_account_variant!(:v2)

    assert Enum.map(v1.contract.columns, & &1.name) == [:account_id, :name, :segment]

    assert Enum.map(v2.contract.columns, & &1.name) ==
             [:account_id, :name, :segment, :industry]

    refute Enum.any?(v1.contract.columns, & &1.nullable?)
    refute Enum.any?(v2.contract.columns, & &1.nullable?)
    refute v1.sql =~ "industry"
    assert v2.sql =~ "industry"

    refute Enum.any?(CRMData.seed(:v1).accounts, &Map.has_key?(&1, :industry))
    assert Enum.all?(CRMData.seed(:v2).accounts, &is_binary(&1.industry))
  end

  defp compile_account_variant!(version) do
    module = Module.concat(__MODULE__, "Accounts#{version}#{System.unique_integer([:positive])}")
    previous = System.get_env("CRM_EXAMPLE_SCHEMA_VERSION")
    System.put_env("CRM_EXAMPLE_SCHEMA_VERSION", Atom.to_string(version))

    source = """
    defmodule #{inspect(module)} do
      use Favn.SQLAsset

      alias FavnReferenceWorkload.SchemaVariant
      require SchemaVariant

      relation(connection: :warehouse, schema: "source", name: "accounts_#{version}")
      materialized(:table)
      execution_pool(:local_duckdb)

      SchemaVariant.accounts_definition()
    end
    """

    try do
      assert [{^module, _binary}] = Code.compile_string(source, "account_schema_variant_test.exs")
      module.__favn_sql_asset_definition__()
    after
      :code.purge(module)
      :code.delete(module)
      restore_schema_version(previous)
    end
  end

  defp restore_schema_version(nil), do: System.delete_env("CRM_EXAMPLE_SCHEMA_VERSION")
  defp restore_schema_version(value), do: System.put_env("CRM_EXAMPLE_SCHEMA_VERSION", value)
end
