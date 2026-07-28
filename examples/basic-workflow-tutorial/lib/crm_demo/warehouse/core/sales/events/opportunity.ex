defmodule CrmDemo.Warehouse.Core.Sales.Events.Opportunity do
  @moduledoc "One row per deal event, enriched with the customer's segment."

  use CrmDemo.SQL.CoreMetadata
  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Source.Crm.Customers.Account
  alias CrmDemo.Warehouse.Source.Crm.Events.Deal

  depends(Deal)
  depends(Account)
  relation(name: "opportunity")
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  execution_pool(:duckdb)

  contract do
    grain(by: [:opportunity_id], description: "one sales opportunity event")

    column(:opportunity_id, :string, null: false)
    column(:customer_id, :string, null: false)
    column(:segment, :string, null: false)
    column(:stage, :string, null: false)
    column(:amount_cents, :integer, null: false)
    column(:occurred_at, :datetime, null: false)

    include(CrmDemo.Contracts.CoreMetadata)
  end

  query(file: "opportunity.sql")
end
