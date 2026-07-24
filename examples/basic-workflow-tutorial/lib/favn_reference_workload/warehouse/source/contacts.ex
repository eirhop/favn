defmodule FavnReferenceWorkload.Warehouse.Source.Contacts do
  @moduledoc "Full-refresh DuckDB source table loaded from landing contacts JSON."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Landing.WriteExtracts

  relation(true)
  depends({WriteExtracts, :contacts_snapshot})
  meta(category: :contacts, tags: [:source, :full_refresh])

  query do
    ~SQL"""
    select
      contact_id,
      account_id,
      name
    from read_json('.data/generic_crm/landing/contacts.json', auto_detect = true)
    """
  end
end
