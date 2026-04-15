defmodule Favn.AssetAndDSLTest do
  use ExUnit.Case, async: true

  alias Favn.Asset.RelationResolver
  alias Favn.DSL.Compiler

  test "infers relation names for module and default asset name" do
    assert RelationResolver.inferred_relation_name_for_module(MyApp.SalesOrders) == "sales_orders"

    asset_like = %{module: MyApp.SalesOrders, name: :daily_orders, relation: nil}
    assert RelationResolver.inferred_relation_name_for_asset(asset_like) == :daily_orders
  end

  test "normalizes docs and relation attr values" do
    assert Compiler.normalize_doc(false) == nil
    assert Compiler.normalize_doc({1, "hello"}) == "hello"
    assert Compiler.valid_relation_attr_value?(true)
    assert Compiler.valid_relation_attr_value?(schema: "public", table: "orders")
    refute Compiler.valid_relation_attr_value?(:invalid)
  end
end
