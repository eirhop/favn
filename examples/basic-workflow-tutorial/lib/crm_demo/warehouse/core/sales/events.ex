defmodule CrmDemo.Warehouse.Core.Sales.Events do
  @moduledoc """
  Reusable sales events for one day.

  These models are windowed because their Source inputs are. The window policy
  is declared once here for the same reason it is on the Source events group.
  """

  use Favn.Namespace

  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  freshness(window_success: true)
  meta(tags: [:daily, :incremental])
end
