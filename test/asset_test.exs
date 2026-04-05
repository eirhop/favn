defmodule Favn.AssetTest do
  use ExUnit.Case, async: true

  alias Favn.Asset
  alias Favn.Ref

  test "defaults optional fields" do
    asset = %Asset{
      module: Example.Assets,
      name: :normalize_orders,
      ref: Ref.new(Example.Assets, :normalize_orders),
      arity: 1,
      file: "lib/example/assets.ex",
      line: 12
    }

    assert asset.meta == %{}
    assert asset.title == nil
    assert asset.depends_on == []
    assert asset.doc == nil
  end

  test "stores the canonical metadata shape" do
    asset = %Asset{
      module: Example.Assets,
      name: :fact_sales,
      ref: Ref.new(Example.Assets, :fact_sales),
      arity: 1,
      doc: "Builds the sales fact table",
      file: "lib/example/assets.ex",
      line: 27,
      title: "Fact Sales",
      meta: %{owner: "analytics", category: :finance, tags: [:warehouse, "finance"]},
      depends_on: [Ref.new(Example.Assets, :normalize_orders)]
    }

    assert asset.module == Example.Assets
    assert asset.name == :fact_sales
    assert asset.ref == {Example.Assets, :fact_sales}
    assert asset.arity == 1
    assert asset.doc == "Builds the sales fact table"
    assert asset.file == "lib/example/assets.ex"
    assert asset.line == 27
    assert asset.title == "Fact Sales"
    assert asset.meta == %{owner: "analytics", category: :finance, tags: [:warehouse, "finance"]}
    assert asset.depends_on == [{Example.Assets, :normalize_orders}]
  end

  test "validate!/1 validates and returns an asset struct" do
    asset = %Asset{
      module: Example.Assets,
      name: :fact_sales,
      ref: Ref.new(Example.Assets, :fact_sales),
      arity: 1,
      doc: "Builds the sales fact table",
      file: "lib/example/assets.ex",
      line: 27,
      title: "Fact Sales",
      meta: %{owner: "analytics", category: :finance, tags: [:warehouse, "finance"]},
      depends_on: [Ref.new(Example.Assets, :normalize_orders)]
    }

    assert Asset.validate!(asset) == asset
  end

  test "validate!/1 rejects invalid meta" do
    assert_raise ArgumentError, ~r/asset meta must be a keyword list or map/, fn ->
      Asset.validate!(%Asset{
        module: Example.Assets,
        name: :bad_meta,
        ref: Ref.new(Example.Assets, :bad_meta),
        arity: 0,
        file: "lib/example/assets.ex",
        line: 10,
        meta: :invalid
      })
    end
  end

  test "validate!/1 rejects invalid canonical depends_on values" do
    assert_raise ArgumentError, ~r/asset depends_on must be a list of Favn\.Ref values/, fn ->
      Asset.validate!(%Asset{
        module: Example.Assets,
        name: :bad_deps,
        ref: Ref.new(Example.Assets, :bad_deps),
        arity: 0,
        file: "lib/example/assets.ex",
        line: 10,
        depends_on: [:not_a_ref]
      })
    end
  end

  test "validate!/1 rejects unsupported metadata keys" do
    assert_raise ArgumentError, ~r/unsupported key/, fn ->
      Asset.validate!(%Asset{
        module: Example.Assets,
        name: :bad_meta_key,
        ref: Ref.new(Example.Assets, :bad_meta_key),
        arity: 0,
        file: "lib/example/assets.ex",
        line: 10,
        meta: %{kind: :legacy}
      })
    end
  end
end
