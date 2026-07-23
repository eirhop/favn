defmodule Favn.PartitionedByDSLTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.PartitionSpec
  alias Favn.SQLAsset.Definition

  defmodule Adapter do
  end

  defmodule Connection do
    @behaviour Favn.Connection

    @impl true
    def definition do
      %Favn.Connection.Definition{
        name: :warehouse,
        adapter: Adapter,
        config_schema: []
      }
    end
  end

  defmodule PartitionedOrders do
    use Favn.SQLAsset

    relation(connection: :warehouse, catalog: "lake", schema: "mart", name: "orders")
    window(Favn.Window.daily())
    materialized({:incremental, strategy: :append})

    partitioned_by([
      :tenant_id,
      {:year, :occurred_at},
      {:month, :occurred_at},
      {:bucket, 32, :account_id}
    ])

    query do
      ~SQL"""
      select
        1 as tenant_id,
        current_timestamp as occurred_at,
        2 as account_id
      """
    end
  end

  test "compiles a structured partition specification into the asset definition" do
    assert %Definition{
             partition_spec: %PartitionSpec{} = spec,
             asset: %{partition_spec: asset_spec}
           } = PartitionedOrders.__favn_sql_asset_definition__()

    assert asset_spec == spec
    assert Enum.map(spec.keys, & &1.transform) == [:identity, :year, :month, :bucket]
  end

  test "preserves the partition specification through manifest serialization" do
    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [PartitionedOrders],
               pipeline_modules: [],
               schedule_modules: [],
               connection_modules: [Connection],
               runner_release: FavnTestSupport.runner_release()
             )

    assert [%Favn.Manifest.Asset{partition_spec: %PartitionSpec{} = expected}] = manifest.assets
    assert {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Favn.Manifest.Serializer.decode_manifest(encoded)
    assert {:ok, rehydrated} = Favn.Manifest.Rehydrate.manifest(decoded)
    assert [%Favn.Manifest.Asset{partition_spec: ^expected}] = rehydrated.assets
  end

  test "rejects partitioning on views and duplicate declarations" do
    assert_raise CompileError, ~r/valid only for table and incremental/, fn ->
      compile_asset!("""
      materialized :view
      partitioned_by [:tenant_id]
      """)
    end

    assert_raise CompileError, ~r/multiple partitioned_by declarations/, fn ->
      compile_asset!("""
      materialized :table
      partitioned_by [:tenant_id]
      partitioned_by [{:month, :occurred_at}]
      """)
    end
  end

  defp compile_asset!(declarations) do
    module = Module.concat(__MODULE__, "Dynamic#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.SQLAsset
        relation connection: :warehouse, schema: "mart"
        #{declarations}
        query do
          ~SQL"select 1 as tenant_id, current_timestamp as occurred_at"
        end
      end
      """,
      "test/partitioned_by_dsl_test.exs"
    )

    module.__favn_sql_asset_definition__()
  end
end
