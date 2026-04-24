defmodule FavnReferenceWorkload.Warehouse.Raw.OrderItems do
  @moduledoc """
  Deterministic order-item ingest from a simulated API JSON payload.

  This Elixir asset models line-item granularity (one row per product in an
  order) by loading JSON rows into DuckDB through `read_json(...)`.

  It depends on:

  - `raw.orders` for valid order IDs
  - `raw.products` for valid product IDs

  This is where later revenue math starts to become possible, because quantity
  and product are both available.

  Alternative:

  - Include discount/tax columns here if you want richer revenue modeling.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  @meta owner: "reference-workload", category: :orders, tags: [:raw, :synthetic]
  @depends FavnReferenceWorkload.Warehouse.Raw.Orders
  @depends FavnReferenceWorkload.Warehouse.Raw.Products
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- FakeAPI.fetch_rows(:order_items) do
      DuckDBJSONLoader.replace_relation_from_rows(
        ctx.asset.relation,
        :order_items,
        rows,
        order_items_select_sql()
      )
    end
  end

  defp order_items_select_sql do
    """
    select
      order_item_id,
      order_id,
      product_id,
      quantity
    from __RAW_JSON_SOURCE__
    where order_id in (
      select order_id
      from raw.orders
    )
      and product_id in (
        select product_id
        from raw.products
      )
    """
  end
end
