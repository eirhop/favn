defmodule FavnReferenceWorkload.Warehouse.Source.Accounts do
  @moduledoc "Full-refresh DuckDB source table loaded from landing accounts JSON."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Landing.WriteExtracts

  relation(true)
  depends({WriteExtracts, :accounts_snapshot})
  meta(category: :accounts, tags: [:source, :full_refresh])

  query do
    ~SQL"""
    select
      account_id,
      name,
      segment
    from read_json('.data/generic_crm/landing/accounts.json', auto_detect = true)
    """
  end
end
