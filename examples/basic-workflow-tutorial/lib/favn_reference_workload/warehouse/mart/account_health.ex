defmodule FavnReferenceWorkload.Warehouse.Mart.AccountHealth do
  @moduledoc "Full-refresh account health table for UI inspection."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Core.Customers

  relation(true)
  depends(Customers)
  meta(category: :account_health, tags: [:mart, :full_refresh])

  contract do
    grain(by: [:customer_id], description: "one health row per customer")
    column(:customer_id, :string, null: false)
    column(:customer_name, :string, null: false)
    column(:segment, :string, null: false)
    column(:contact_count, :integer, null: false)
    column(:health_status, :string, null: false)
    unique([:customer_id])
    row_count(min: 1, on_violation: :fail)
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
