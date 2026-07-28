defmodule CrmDemo.Warehouse.Source.Crm.Customers.Contact do
  @moduledoc "Typed CRM contacts, published from one completed landing snapshot."

  use CrmDemo.SQL.SourceMetadata
  use Favn.SQLAsset

  alias CrmDemo.Landing.Crm.Snapshots

  settings(landing_dataset: "contacts")
  depends({Snapshots, :contacts})
  relation(name: "contact")
  execution_pool(:duckdb)

  contract do
    grain(by: [:contact_id], description: "one CRM contact")

    column(:contact_id, :string, null: false)
    column(:account_id, :string, null: false)
    column(:full_name, :string, null: false)

    include(CrmDemo.Contracts.SourceMetadata)

    row_count(equals: param(:expected_row_count), on_violation: :fail)
  end

  query(file: "contact.sql")
end
