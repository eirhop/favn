defmodule CrmDemo.Warehouse.Mart.Sales.AccountHealth do
  @moduledoc """
  One row per customer with a derived health label.

  `check` is for rules that need arbitrary SQL. Everything a contract can
  already express - nullability, grain, keys, row counts - belongs in the
  contract, where tooling can read it.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Core.Sales.Customers.Customer

  depends(Customer)
  relation(name: "account_health")
  execution_pool(:duckdb)

  contract do
    grain(by: [:customer_id], description: "one customer")

    column(:customer_id, :string, null: false)
    column(:customer_name, :string, null: false)
    column(:segment, :string, null: false)
    column(:contact_count, :integer, null: false)
    column(:health_status, :string, null: false)

    row_count(min: 1, on_violation: :fail)
  end

  check :health_status_is_known,
    at: :before_materialize,
    on_violation: :fail,
    message: "health_status must be one of the labels this mart defines" do
    ~SQL"""
    select
      count(*) filter (where health_status not in ('engaged', 'needs_attention')) = 0 as passed,
      count(*) filter (where health_status not in ('engaged', 'needs_attention')) as invalid_rows
    from query()
    """
  end

  query(file: "account_health.sql")
end
