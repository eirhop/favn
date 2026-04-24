defmodule FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview do
  @moduledoc """
  One-row executive KPI snapshot for the reference workload.

  This is the top business summary table in the graph.

  It combines multiple gold/stg outputs into one "dashboard-like" row:

  - latest daily revenue
  - top channel and its revenue
  - product daily coverage count
  - total customer count and latest customer activity date

  Alternative:

  - Expose each KPI as separate assets and let dashboards compose them.
  - Keep this single-row table for a simple operator or demo entrypoint.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :executive, tags: [:gold, :summary]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    with
      revenue as (
        select order_date, revenue_cents
        from gold.revenue_daily
      ),
      channel as (
        select channel_code, revenue_cents
        from gold.channel_efficiency
      ),
      product as (
        select count(*) as product_day_rows
        from stg.product_daily
      ),
      customers as (
        select
          count(*) as total_customers,
          max(last_order_date) as latest_customer_activity
        from gold.customer_360
      )
    select
      revenue.order_date as snapshot_date,
      revenue.revenue_cents as daily_revenue_cents,
      efficiency.channel_code as top_channel,
      efficiency.revenue_cents as top_channel_revenue_cents,
      product.product_day_rows,
      customers.total_customers,
      customers.latest_customer_activity
    from revenue
    inner join channel as efficiency
      on 1 = 1
    cross join product
    cross join customers
    order by revenue.order_date desc, efficiency.revenue_cents desc
    limit 1
    """
  end
end
