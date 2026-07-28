defmodule CrmDemo.Warehouse.Mart.Sales.PipelineDaily do
  @moduledoc """
  Deal count and value per stage per day.

  The window column is `snapshot_date`, not `occurred_at`: an incremental write
  plan replaces rows by the column the target is actually partitioned on, which
  after aggregation is the date.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Core.Sales.Events.Opportunity

  depends(Opportunity)
  relation(name: "pipeline_daily")
  window(Favn.Window.daily(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2026-07-22], through: :latest_closed, availability_delay: {:hours, 1})
  freshness(window_success: true)
  materialized({:incremental, strategy: :delete_insert, window_column: :snapshot_date})
  execution_pool(:duckdb)

  contract do
    grain(by: [:snapshot_date, :stage], description: "one stage total for one day")

    column(:snapshot_date, :date, null: false)
    column(:stage, :string, null: false)
    column(:deal_count, :integer, null: false)
    column(:pipeline_amount_cents, :integer, null: false)
  end

  query(file: "pipeline_daily.sql")
end
