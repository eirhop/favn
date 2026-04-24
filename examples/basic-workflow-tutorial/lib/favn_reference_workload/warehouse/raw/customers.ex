defmodule FavnReferenceWorkload.Warehouse.Raw.Customers do
  @moduledoc """
  Deterministic customer ingest from a simulated API JSON payload.

  This is an Elixir asset (`use Favn.Asset`) that simulates an API client call,
  writes the JSON response to a local payload file, and loads that payload into
  DuckDB with `read_json(...)`.

  Why this module exists:

  - creates stable tutorial data (same rows every run)
  - demonstrates JSON-to-DuckDB ingestion in Elixir assets
  - keeps relation ownership in the `raw` layer

  DSL walkthrough:

  - `use Favn.Namespace` is enough here because connection and schema are
    inherited from parent namespace modules.
  - `@relation true` uses default relation name `customers`.
  - `@depends` declares upstream requirements for this Elixir asset.
  - the asset passes only `ctx.asset.relation` into the DuckDB client, because
    that client only needs relation metadata.

  Client split shown here:

  - `FavnReferenceWorkload.Client.FakeAPI` returns deterministic JSON rows and
    does not need `ctx` at all
  - `FavnReferenceWorkload.Client.DuckDBJSONLoader` receives only the extracted
    relation metadata it needs to create `raw.customers`

  Alternative:

  - You can set `@relation [name: "raw_customers"]` for explicit naming.
  - You can switch back to `Favn.SQLAsset` if you want all logic inline as SQL.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  @meta owner: "reference-workload", category: :customers, tags: [:raw, :synthetic]
  @depends FavnReferenceWorkload.Warehouse.Sources.CountryRegions
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- FakeAPI.fetch_rows(:customers) do
      DuckDBJSONLoader.replace_relation_from_rows(
        ctx.asset.relation,
        :customers,
        rows,
        customers_select_sql()
      )
    end
  end

  defp customers_select_sql do
    """
    select
      customer_id,
      customer_code,
      region_code,
      country_code,
      cast(signup_date as date) as signup_date
    from __RAW_JSON_SOURCE__
    where region_code in (
      select region_code
      from sources.country_regions
    )
    """
  end
end
