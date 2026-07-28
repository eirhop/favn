defmodule CrmDemo.Warehouse.Source.Crm.Customers.Account do
  @moduledoc """
  Typed CRM accounts, published from one completed landing snapshot.

  This is the canonical shape of a Source asset: one dependency, one relation,
  one contract, one adjacent SQL file. The contract's column order is the same
  order `account.sql` projects.
  """

  use CrmDemo.SQL.SourceMetadata
  use Favn.SQLAsset

  alias CrmDemo.Landing.Crm.Snapshots

  settings(landing_dataset: "accounts")
  depends({Snapshots, :accounts})
  relation(name: "account")
  execution_pool(:duckdb)

  contract do
    grain(by: [:account_id], description: "one CRM account")

    column(:account_id, :string, null: false)
    column(:name, :string, null: false)
    column(:segment, :string, null: false)
    column(:industry, :string, null: false)

    include(CrmDemo.Contracts.SourceMetadata)

    row_count(equals: param(:expected_row_count), on_violation: :fail)
  end

  query(file: "account.sql")
end
