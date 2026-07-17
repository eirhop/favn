defmodule FavnReferenceWorkload.Warehouse.Raw do
  @moduledoc """
  Namespace defaults for raw-layer assets.

  This sets the `raw` schema for child modules. The parent warehouse namespace
  supplies the connection and owner metadata.

  Best-practice namespace pattern:

  - parent namespace modules define shared defaults once
  - child asset modules inherit those defaults through module ancestry
  - child modules should not repeat `connection` and `schema` unless they are
    intentionally overriding them

  Raw assets in this workload are Elixir assets that:

  - call `FavnReferenceWorkload.Client.FakeAPI` to simulate API JSON responses
  - call `FavnReferenceWorkload.Client.DuckDBJSONLoader` to load that JSON into
    DuckDB `raw.*` relations with `read_json(...)`
  """

  use Favn.Namespace

  relation schema: "raw"
end
