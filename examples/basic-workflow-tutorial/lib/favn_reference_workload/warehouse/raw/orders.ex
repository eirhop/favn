defmodule FavnReferenceWorkload.Warehouse.Raw.Orders do
  @moduledoc """
  Deterministic order ingest from a simulated API JSON payload.

  This Elixir asset loads API JSON into `raw.orders` and validates two
  foreign-key-like constraints in SQL:

  - customer must exist in `raw.customers`
  - channel must exist in `sources.channel_catalog`

  In simple terms: this module shows how to keep synthetic data believable while
  still staying deterministic.

  Alternative:

  - Add explicit `@depends` only when dependencies are not visible in SQL.
  """

  use Favn.Namespace
  use Favn.Asset

  alias FavnReferenceWorkload.Client.DuckDBJSONLoader
  alias FavnReferenceWorkload.Client.FakeAPI

  @meta owner: "reference-workload", category: :orders, tags: [:raw, :synthetic]
  @depends FavnReferenceWorkload.Warehouse.Raw.Customers
  @depends FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog
  @relation true
  def asset(ctx) do
    with {:ok, rows} <- FakeAPI.fetch_rows(:orders) do
      DuckDBJSONLoader.replace_relation_from_rows(
        ctx.asset.relation,
        :orders,
        rows,
        orders_select_sql()
      )
    end
  end

  defp orders_select_sql do
    """
    select
      order_id,
      customer_id,
      channel_code,
      cast(order_date as date) as order_date
    from __RAW_JSON_SOURCE__
    where customer_id in (
      select customer_id
      from raw.customers
    )
      and channel_code in (
        select channel_code
        from sources.channel_catalog
      )
    """
  end
end
