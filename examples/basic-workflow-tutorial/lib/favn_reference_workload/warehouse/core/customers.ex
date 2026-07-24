defmodule FavnReferenceWorkload.Warehouse.Core.Customers do
  @moduledoc "System-agnostic customer model assembled from CRM source tables."

  use Favn.SQLAsset

  alias FavnReferenceWorkload.Warehouse.Source.{Accounts, Contacts}

  relation(true)
  depends(Accounts)
  depends(Contacts)
  meta(category: :customers, tags: [:core, :full_refresh])

  query do
    ~SQL"""
    select
      accounts.account_id as customer_id,
      accounts.name as customer_name,
      accounts.segment,
      count(contacts.contact_id) as contact_count
    from source.accounts as accounts
    left join source.contacts as contacts
      on contacts.account_id = accounts.account_id
    group by accounts.account_id, accounts.name, accounts.segment
    """
  end
end
