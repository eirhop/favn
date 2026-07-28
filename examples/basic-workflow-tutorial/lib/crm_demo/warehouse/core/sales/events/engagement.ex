defmodule CrmDemo.Warehouse.Core.Sales.Events.Engagement do
  @moduledoc "One row per customer activity, enriched with the customer's segment."

  use CrmDemo.SQL.CoreMetadata
  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Source.Crm.Customers.Account
  alias CrmDemo.Warehouse.Source.Crm.Events.Activity

  depends(Activity)
  depends(Account)
  relation(name: "engagement")
  materialized({:incremental, strategy: :delete_insert, window_column: :occurred_at})
  execution_pool(:duckdb)

  contract do
    grain(by: [:engagement_id], description: "one customer engagement event")

    column(:engagement_id, :string, null: false)
    column(:customer_id, :string, null: false)
    column(:segment, :string, null: false)
    column(:engagement_type, :string, null: false)
    column(:occurred_at, :datetime, null: false)

    include(CrmDemo.Contracts.CoreMetadata)
  end

  query(file: "engagement.sql")
end
