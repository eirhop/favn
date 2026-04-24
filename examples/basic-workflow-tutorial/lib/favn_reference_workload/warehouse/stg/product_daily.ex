defmodule FavnReferenceWorkload.Warehouse.Stg.ProductDaily do
  @moduledoc """
  Daily product performance aggregate.

  This staging aggregate summarizes units and gross revenue per product per day.

  In simple terms: it turns detailed row-level events into daily product-level
  metrics.

  Alternative:

  - promote this to a gold asset if your organization treats product-daily as a
    public business KPI output.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :products, tags: [:stg, :daily]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    select
      order_date,
      product_id,
      product_category,
      sum(quantity) as units_sold,
      sum(gross_item_amount_cents) as gross_revenue_cents
    from stg.order_facts
    group by order_date, product_id, product_category
    """
  end
end
