defmodule FavnReferenceWorkload.Warehouse.Sources.CountryRegions do
  @moduledoc """
  Deterministic region lookup seed used by synthetic customer generation.

  This module is a small Elixir seed asset. It creates the lookup table that
  downstream raw assets read when validating synthetic customer rows.

  Key DSL pieces:

  - relation location is inherited from parent namespaces (`:warehouse`
    connection from `Warehouse`, `sources` schema from `Warehouse.Sources`).
  - `@relation true` means use the default relation name based on module name
    (`country_regions`).
  - `asset/1` writes deterministic rows into the owned DuckDB relation.
  - `@meta ...` adds searchable ownership/category/tags metadata.

  Alternative:

  - Use `@relation [name: "my_regions"]` to override the default relation name.
  - Use `Favn.Source` only when this table is managed outside Favn and should not
    be created by the workload.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader

  @meta owner: "reference-workload", category: :reference_data, tags: [:seed]
  @relation true
  def asset(ctx) do
    DuckDBJSONLoader.replace_relation_from_sql(ctx.asset.relation, """
    select region_code
    from (
      values ('nordic'), ('dach'), ('uk_ie'), ('southern_eu')
    ) as t(region_code)
    """)
  end
end
