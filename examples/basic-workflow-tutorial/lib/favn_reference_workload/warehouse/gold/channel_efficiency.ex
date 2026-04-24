defmodule FavnReferenceWorkload.Warehouse.Gold.ChannelEfficiency do
  @moduledoc """
  Channel performance summary for acquisition analysis.

  This gold asset compares channels by:

  - number of orders
  - number of customers
  - total revenue
  - average customer lifetime value proxy

  It demonstrates a common analytics pattern: join one KPI table with another
  business summary (`gold.customer_360`) to create richer channel metrics.

  Alternative:

  - Keep channel metrics independent from customer LTV if you want a simpler
    attribution model.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :marketing, tags: [:gold]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    select
      facts.channel_code,
      count(distinct facts.order_id) as orders_count,
      count(distinct facts.customer_id) as customers_count,
      sum(facts.payment_amount_cents) as revenue_cents,
      avg(c360.lifetime_revenue_cents) as avg_customer_ltv_cents
    from stg.order_facts as facts
    inner join gold.customer_360 as c360
      on facts.customer_id = c360.customer_id
    group by facts.channel_code
    """
  end
end
