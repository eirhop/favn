defmodule CrmDemo.Warehouse.Core.Sales.Customers.Customer do
  @moduledoc """
  One row per customer, with the contact count resolved from CRM data.

  Note what changed at this boundary: the columns are named for the business
  (`customer_id`), not for the source (`account_id`), and the technical columns
  come from the Core fragment instead of the Source one.
  """

  use CrmDemo.SQL.CoreMetadata
  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Source.Crm.Customers.{Account, Contact}

  depends(Account)
  depends(Contact)
  relation(name: "customer")
  execution_pool(:duckdb)

  contract do
    grain(by: [:customer_id], description: "one customer")

    column(:customer_id, :string, null: false)
    column(:customer_name, :string, null: false)
    column(:segment, :string, null: false)
    column(:industry, :string, null: false)
    column(:contact_count, :integer, null: false)

    include(CrmDemo.Contracts.CoreMetadata)

    row_count(min: 1, on_violation: :fail)
  end

  query(file: "customer.sql")
end
