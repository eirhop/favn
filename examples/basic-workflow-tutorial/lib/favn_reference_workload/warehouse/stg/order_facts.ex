defmodule FavnReferenceWorkload.Warehouse.Stg.OrderFacts do
  @moduledoc """
  Joined order-level fact model across orders, items, products, and payments.

  This is the central analytics fact table in the tutorial.

  Why it matters:

  - most downstream KPI assets read from this table
  - it combines business dimensions (channel, customer tier, category) and
    measures (quantity, amount)

  Query behavior to notice:

  - joins to `raw.payments` keep only `payment_status = 'succeeded'`
  - revenue measures come from payment/item math fields

  Alternative:

  - build separate facts (orders fact, payments fact, items fact) if you prefer
    thinner, more specialized models.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :orders, tags: [:stg, :facts]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    select
      orders.order_id,
      orders.customer_id,
      customers.market_tier,
      orders.channel_code,
      orders.order_date,
      items.order_item_id,
      items.product_id,
      products.category as product_category,
      items.quantity,
      products.unit_price_cents,
      items.quantity * products.unit_price_cents as gross_item_amount_cents,
      payments.payment_id,
      payments.paid_at,
      payments.amount_cents as payment_amount_cents
    from raw.orders as orders
    inner join raw.order_items as items
      on orders.order_id = items.order_id
    inner join raw.products as products
      on items.product_id = products.product_id
    inner join raw.payments as payments
      on orders.order_id = payments.order_id
     and payments.payment_status = 'succeeded'
    inner join stg.customers as customers
      on orders.customer_id = customers.customer_id
    """
  end
end
