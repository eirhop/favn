Code.require_file(Path.join(:code.priv_dir(:favn_test_support), "fixtures/catalog.exs"))

defmodule Favn.Catalog.ProjectionTest do
  use ExUnit.Case, async: true
  alias Favn.Catalog.Projection
  alias FavnTestSupport.CatalogFixture, as: Fixture

  test "typed discovery and ordered relations preserve the original artifact" do
    artifact = Fixture.rich_semantic()
    assert {:ok, projection} = Projection.build(artifact)
    assert {:ok, json} = Favn.Semantic.Artifact.encode(artifact)
    assert projection.document == json
    assert projection.version == artifact.semantic_version
    rows = records(projection)
    metric = Enum.find(rows["metric"], &(&1["ref"] == "sales.revenue"))

    assert Map.take(
             metric,
             ~w(description unit_kind unit_value format_style format_decimals time_aggregate)
           ) == %{
             "description" => "Net sales revenue",
             "unit_kind" => "currency",
             "unit_value" => "NOK",
             "format_style" => "currency",
             "format_decimals" => 2,
             "time_aggregate" => "aggregate"
           }

    assert [%{"dependency_ref" => "sales.revenue", "metric_ref" => "sales.doubled"}] =
             rows["metric_dependency"]

    assert Enum.map(rows["dimension_key"], &{&1["ordinal"], &1["column"]}) == [
             {1, "tenant_id"},
             {2, "store_id"}
           ]

    assert Enum.map(rows["hierarchy_level"], &{&1["hierarchy"], &1["ordinal"], &1["column"]}) == [
             {"geography", 1, "country"},
             {"geography", 2, "tenant_id"},
             {"geography", 3, "store_id"}
           ]

    assert Enum.map(rows["metric_entity_key"], &{&1["metric_ref"], &1["ordinal"], &1["column"]}) ==
             [
               {"sales.closing", 1, "tenant_id"},
               {"sales.closing", 2, "sale_id"}
             ]

    assert [%{"name" => "store", "model" => "stores", "label_column" => "store_label"}] =
             rows["dimension"]

    assert Enum.find(rows["model"], &(&1["name"] == "sales"))["time_timezone"] == "Europe/Oslo"
    assert Enum.find(rows["model"], &(&1["name"] == "sales"))["time_grain"] == "day"
    assert Enum.find(rows["model"], &(&1["name"] == "sales"))["time_column"] == "sale_date"

    for relationship <- rows["relationship"] do
      assert relationship["target_asset_ref"] == "Example.Store.asset"
      assert relationship["cardinality"] == "many_to_one"

      assert relationship["on_violation"] ==
               if(relationship["name"] == "store", do: "fail", else: "warn")

      keys =
        Enum.filter(rows["relationship_key"], &(&1["relationship_name"] == relationship["name"]))

      assert Enum.map(keys, &{&1["ordinal"], &1["source_column"], &1["target_column"]}) == [
               {1, "tenant_id", "tenant_id"},
               {2, "store_id", "store_id"}
             ]
    end

    for model <- artifact.models, metric <- model["metrics"] do
      rows = Enum.filter(rows["metric_minimum_grain"], &(&1["metric_ref"] == metric["ref"]))

      assert Enum.map(rows, &{&1["relationship_name"], &1["ordinal"]}) ==
               Enum.with_index(metric["minimum_grain"], 1)
    end

    assert Enum.all?(projection.tables, fn {_, entries} ->
             Enum.all?(entries, fn [context, version | _] ->
               context == "semantic" and version == artifact.semantic_version
             end)
           end)
  end

  test "manifest contracts expose the same ordered relationship rows in their own context" do
    {:ok, manifest} = Projection.build(Fixture.rich_manifest())
    {:ok, semantic} = Projection.build(Fixture.rich_semantic())

    for table <- ~w(relationship relationship_key) do
      assert Enum.map(manifest.tables[table], fn [context, version | fields] ->
               assert context == "manifest"
               assert version == manifest.version
               fields
             end) == Enum.map(semantic.tables[table], fn [_, _ | fields] -> fields end)
    end
  end

  test "optional values stay null and absent declarations create no rows" do
    {:ok, projection} = Projection.build(Fixture.semantic())
    rows = records(projection)

    assert [
             %{
               "unit_kind" => "ratio",
               "unit_value" => nil,
               "format_style" => nil,
               "format_decimals" => nil
             }
           ] = rows["metric"]

    assert [%{"time_column" => nil, "time_grain" => nil, "time_timezone" => nil}] = rows["model"]

    for table <-
          ~w(dimension dimension_key hierarchy_level metric_minimum_grain metric_entity_key metric_dependency relationship relationship_key),
        do: assert(rows[table] == [])

    {:ok, rich} = Projection.build(Fixture.rich_semantic())
    metric = Enum.find(records(rich)["metric"], &(&1["ref"] == "sales.closing"))
    assert metric["unit_value"] == nil
    assert metric["format_style"] == "number"
    assert metric["format_decimals"] == nil
  end

  defp records(projection) do
    Map.new(projection.tables, fn {table, rows} ->
      keys = Enum.map(Projection.columns()[table], &elem(&1, 0))
      {table, Enum.map(rows, &Map.new(Enum.zip(keys, &1)))}
    end)
  end
end
