defmodule FavnReferenceWorkload.Warehouse.Raw.Products do
  @moduledoc """
  Deterministic product ingest from a simulated API JSON payload.

  This Elixir asset receives a fixed JSON payload from the simulated API client
  and loads it into DuckDB `raw.products` through `read_json(...)`.

  Practical purpose:

  - keeps tutorial runs deterministic
  - provides product pricing/category dimensions for later metrics

  Alternative:

  - Replace static `values` rows with source-table ingestion when integrating
    real systems.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  @meta owner: "reference-workload", category: :products, tags: [:raw, :synthetic]
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- FakeAPI.fetch_rows(:products) do
      DuckDBJSONLoader.replace_relation_from_rows(
        ctx.asset.relation,
        :products,
        rows,
        products_select_sql()
      )
    end
  end

  defp products_select_sql do
    """
    select
      product_id,
      sku,
      product_name,
      category,
      unit_price_cents
    from __RAW_JSON_SOURCE__
    """
  end
end
