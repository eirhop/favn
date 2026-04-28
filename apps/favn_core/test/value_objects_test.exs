defmodule Favn.ValueObjectsTest do
  use ExUnit.Case, async: true

  test "builds canonical asset refs" do
    assert Favn.Ref.new(MyApp.Asset, :asset) == {MyApp.Asset, :asset}
  end

  test "normalizes relation refs including aliases" do
    relation = Favn.RelationRef.new!(database: "raw", schema: :sales, table: :orders)

    assert relation.catalog == "raw"
    assert relation.schema == "sales"
    assert relation.name == "orders"
  end

  test "validates timezone identifiers" do
    assert Favn.Timezone.valid_identifier?("Etc/UTC")
    assert Favn.Timezone.valid_identifier?("Europe/Oslo")
    refute Favn.Timezone.valid_identifier?("../etc/passwd")
    refute Favn.Timezone.valid_identifier?("/etc/passwd")
    refute Favn.Timezone.valid_identifier?("Not/AZone")
  end
end
