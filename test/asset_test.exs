defmodule Flux.AssetTest do
  use ExUnit.Case, async: true

  alias Flux.Asset
  alias Flux.Ref

  require Logger

  test "defaults optional fields" do
    asset = %Asset{
      module: Example.Assets,
      name: :normalize_orders,
      ref: Ref.new(Example.Assets, :normalize_orders),
      arity: 1,
      file: "lib/example/assets.ex",
      line: 12
    }

    Logger.debug("default asset metadata: #{inspect(asset, pretty: true)}")

    assert asset.kind == :materialized
    assert asset.tags == []
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
      kind: :view,
      tags: [:warehouse, "finance"],
      depends_on: [Ref.new(Example.Assets, :normalize_orders)]
    }

    Logger.debug("canonical asset metadata: #{inspect(asset, pretty: true)}")

    assert asset.module == Example.Assets
    assert asset.name == :fact_sales
    assert asset.ref == {Example.Assets, :fact_sales}
    assert asset.arity == 1
    assert asset.doc == "Builds the sales fact table"
    assert asset.file == "lib/example/assets.ex"
    assert asset.line == 27
    assert asset.kind == :view
    assert asset.tags == [:warehouse, "finance"]
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
      kind: :view,
      tags: [:warehouse, "finance"],
      depends_on: [Ref.new(Example.Assets, :normalize_orders)]
    }

    assert Asset.validate!(asset) == asset
  end

  test "validate!/1 rejects an invalid kind" do
    assert_raise ArgumentError, ~r/invalid asset kind/, fn ->
      Asset.validate!(%Asset{
        module: Example.Assets,
        name: :bad_kind,
        ref: Ref.new(Example.Assets, :bad_kind),
        arity: 0,
        file: "lib/example/assets.ex",
        line: 10,
        kind: :invalid
      })
    end
  end

  test "validate!/1 rejects invalid tags" do
    assert_raise ArgumentError, ~r/asset tags must be atoms or strings/, fn ->
      Asset.validate!(%Asset{
        module: Example.Assets,
        name: :bad_tags,
        ref: Ref.new(Example.Assets, :bad_tags),
        arity: 0,
        file: "lib/example/assets.ex",
        line: 10,
        tags: [:ok, 1]
      })
    end
  end

  test "validate!/1 rejects invalid canonical depends_on values" do
    assert_raise ArgumentError, ~r/asset depends_on must be a list of Flux\.Ref values/, fn ->
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
end
