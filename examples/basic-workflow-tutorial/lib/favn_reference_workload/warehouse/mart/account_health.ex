defmodule FavnReferenceWorkload.Warehouse.Mart.AccountHealth do
  @moduledoc "Full-refresh account health table for UI inspection."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Core.Customers

  relation(true)
  execution_pool(:local_duckdb)
  depends(Customers)
  meta(category: :account_health, tags: [:mart, :full_refresh])

  contract do
    column(:customer_id, :string)
    column(:customer_name, :string)
    column(:segment, :string)
    column(:contact_count, :integer)
    column(:health_status, :string)
    unique([:customer_id])
    row_count(min: 1, on_violation: :fail)
  end

  check :required_values_are_present,
    at: :before_materialize,
    on_violation: :fail,
    message: "Account health rows must contain every required value" do
    ~SQL"""
    select
      count(*) filter (
        where customer_id is null
          or customer_name is null
          or segment is null
          or contact_count is null
          or health_status is null
      ) = 0 as passed
    from query()
    """
  end

  check :has_accounts,
    at: :before_materialize,
    on_violation: :fail,
    message: "The demo must contain at least one account" do
    ~SQL"select count(*) > 0 as passed, count(*) as account_count from query()"
  end

  query do
    ~SQL"""
    select
      customer_id,
      customer_name,
      segment,
      contact_count,
      case
        when contact_count >= 1 then 'engaged'
        else 'needs_attention'
      end as health_status
    from core.customers
    """
  end
end
