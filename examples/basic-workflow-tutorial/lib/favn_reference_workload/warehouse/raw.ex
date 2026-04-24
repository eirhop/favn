defmodule FavnReferenceWorkload.Warehouse.Raw do
  @moduledoc """
  Namespace defaults for raw-layer assets.

  This sets the `:warehouse` connection and `raw` schema for child modules.

  Best-practice namespace pattern:

  - parent namespace modules define shared defaults once
  - child asset modules use bare `use Favn.Namespace` to inherit those defaults
  - child modules should not repeat `connection` and `schema` unless they are
    intentionally overriding them

  Raw assets in this workload are Elixir assets that:

  - call `FavnReferenceWorkload.Client.FakeAPI` to simulate API JSON responses
  - call `FavnReferenceWorkload.Client.DuckDBJSONLoader` to load that JSON into
    DuckDB `raw.*` relations with `read_json(...)`
  """

  use Favn.Namespace, relation: [connection: :warehouse, schema: "raw"]
end
