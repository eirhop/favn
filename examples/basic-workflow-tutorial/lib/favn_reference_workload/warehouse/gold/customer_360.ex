defmodule FavnReferenceWorkload.Warehouse.Gold.Customer360 do
  @moduledoc """
  Customer lifecycle and value summary.

  This gold asset builds one row per customer with high-value business metrics:

  - total orders
  - lifetime revenue
  - first and latest order dates

  DSL walkthrough in simple terms:

  - `@relation [name: ...]` overrides only the table name while connection and
    schema are inherited from parent namespaces.
  - `use Favn.SQLAsset` means this asset is implemented by SQL.
  - `@materialized :table` stores the result as a table.
  - `@relation [name: "customer_360"]` explicitly sets output table name.

  Alternative naming behavior:

  - If you remove `@relation [name: "customer_360"]`, Favn uses the default
    relation name from the module and the table name becomes `customer360`.

  Alternative layering behavior:

  - If you do not need a custom table name, set `@relation true`.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :customers, tags: [:gold]
  @relation [name: "customer_360"]
  @materialized :table

  query do
    ~SQL"""
    select
      customers.customer_id,
      customers.customer_code,
      customers.region_code,
      customers.market_tier,
      customers.signup_date,
      count(distinct facts.order_id) as total_orders,
      coalesce(sum(facts.payment_amount_cents), 0) as lifetime_revenue_cents,
      min(facts.order_date) as first_order_date,
      max(facts.order_date) as last_order_date
    from stg.customers as customers
    left join stg.order_facts as facts
      on customers.customer_id = facts.customer_id
    group by
      customers.customer_id,
      customers.customer_code,
      customers.region_code,
      customers.market_tier,
      customers.signup_date
    """
  end
end
