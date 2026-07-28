defmodule CrmDemo.Landing.Crm.Snapshots do
  @moduledoc """
  Lands the CRM reference data that has no useful time dimension.

  Both datasets re-read the whole endpoint on every run. `freshness(:always)`
  says so explicitly: there is no window to satisfy, so the assets never count
  as already fresh.
  """

  use Favn.MultiAsset

  alias CrmDemo.Landing.Crm.Extractor

  settings(mode: "full_refresh", page_size: 3)
  freshness(:always)
  meta(tags: [:landing, :full_refresh])

  asset :accounts do
    description("Land every CRM account.")
    settings(dataset: "accounts", endpoint: "Accounts")
  end

  asset :contacts do
    description("Land every CRM contact.")
    settings(dataset: "contacts", endpoint: "Contacts")
  end

  @doc "Extracts one full CRM snapshot into landing storage."
  def asset(ctx), do: Extractor.extract(ctx)
end
