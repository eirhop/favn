defmodule CrmDemo.Warehouse.Source.Crm.Events do
  @moduledoc """
  CRM event data: what happened, and when.

  The window, coverage, and freshness policy are identical for every relation
  here, so they are declared once. Each leaf still owns its own dependency,
  relation name, contract, and SQL.
  """

  use Favn.Namespace

  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  freshness(window_success: true)
  meta(tags: [:daily, :incremental])
end
