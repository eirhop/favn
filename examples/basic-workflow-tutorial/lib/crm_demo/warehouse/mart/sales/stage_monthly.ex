defmodule CrmDemo.Warehouse.Mart.Sales.StageMonthly do
  @moduledoc """
  Deal count and value per stage per calendar month.

  Coverage starts far enough back to span two calendar years, so the months are
  reached by stepping between years rather than all sitting on one screen.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Core.Sales.Events.Opportunity

  depends(Opportunity)
  relation(name: "stage_monthly")
  window(Favn.Window.monthly(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2025-06-01], through: :latest_closed, availability_delay: {:days, 2})
  freshness(window_success: true)
  materialized({:incremental, strategy: :delete_insert, window_column: :bucket_month})
  execution_pool(:duckdb)

  meta(tags: [:monthly])

  contract do
    grain(by: [:bucket_month, :stage], description: "one stage total for one month")

    column(:bucket_month, :date, null: false)
    column(:stage, :string, null: false)
    column(:deal_count, :integer, null: false)
    column(:pipeline_amount_cents, :integer, null: false)
  end

  query(file: "stage_monthly.sql")
end
