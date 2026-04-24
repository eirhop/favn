defmodule FavnReferenceWorkload.Warehouse.Sources.CountryRegions do
  @moduledoc """
  External region lookup source used by synthetic customer generation.

  This module uses `Favn.Source`, which means it declares an available relation
  in the catalog rather than running transformation SQL.

  Key DSL pieces:

  - relation location is inherited from parent namespaces (`:warehouse`
    connection from `Warehouse`, `sources` schema from `Warehouse.Sources`).
  - `@relation true` means use the default relation name based on module name
    (`country_regions`).
  - `@meta ...` adds searchable ownership/category/tags metadata.

  Alternative:

  - Use `@relation [name: "my_regions"]` to override the default relation name.
  """

  use Favn.Namespace
  use Favn.Source

  @meta owner: "reference-workload", category: :reference_data, tags: [:source]
  @relation true
end
