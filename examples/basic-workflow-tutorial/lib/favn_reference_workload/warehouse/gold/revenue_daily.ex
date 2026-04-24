defmodule FavnReferenceWorkload.Warehouse.Gold.RevenueDaily do
  @moduledoc """
  Daily revenue KPI output.

  This gold asset answers a core business question: "How much revenue did we
  make each day, and from how many orders/customers?"

  It aggregates from `stg.order_facts` and emits one row per `order_date`.

  Alternative:

  - Add window-level variants (weekly/monthly) as separate assets if you need
    multiple KPI granularities.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :revenue, tags: [:gold, :daily]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    select
      order_date,
      count(distinct order_id) as orders_count,
      count(distinct customer_id) as active_customers,
      sum(payment_amount_cents) as revenue_cents
    from stg.order_facts
    group by order_date
    """
  end
end
