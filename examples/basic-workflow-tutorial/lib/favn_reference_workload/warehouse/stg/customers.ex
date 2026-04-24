defmodule FavnReferenceWorkload.Warehouse.Stg.Customers do
  @moduledoc """
  Normalized customer attributes for analytics modeling.

  This staging asset standardizes customer fields and derives `market_tier`.

  In plain language:

  - raw tables are often messy or inconsistent
  - staging tables make them easier to analyze safely

  Alternative:

  - Keep only normalization here and move business logic (like `market_tier`)
    to a gold model if your team prefers stricter layering.
  """

  use Favn.Namespace
  use Favn.SQLAsset

  @meta owner: "reference-workload", category: :customers, tags: [:stg]
  @materialized :table
  @relation true

  query do
    ~SQL"""
    select
      customer_id,
      customer_code,
      lower(region_code) as region_code,
      upper(country_code) as country_code,
      signup_date,
      case
        when country_code in ('NO', 'SE', 'DE') then 'core'
        else 'growth'
      end as market_tier
    from raw.customers
    """
  end
end
