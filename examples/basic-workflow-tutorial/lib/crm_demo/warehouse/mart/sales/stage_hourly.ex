defmodule CrmDemo.Warehouse.Mart.Sales.StageHourly do
  @moduledoc """
  Deal count and value per stage per hour.

  The finest grain Favn offers, and the one an operator surface has to get right:
  a day is 23, 24, or 25 hours long depending on the clock change, so anything
  that assumes 24 is wrong twice a year.

  Coverage is a fixed two-day range rather than `:latest_closed`, because hours
  accumulate at 24 a day and a demo that grows without bound stops being a demo.
  A fixed end also rules out an availability delay: there is no "latest closed"
  period for the delay to hold back. The bounds are instants rather than dates,
  because a date cannot name which hour it means, and the range ends on the last
  hour of a day rather than the first hour of the next so that the newest screen
  is a whole day rather than a single cell.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Core.Sales.Events.Opportunity

  depends(Opportunity)
  relation(name: "stage_hourly")
  window(Favn.Window.hourly(timezone: "Etc/UTC", required: true))
  coverage(from: ~U[2026-07-22 00:00:00Z], through: ~U[2026-07-23 23:00:00Z])
  freshness(window_success: true)
  materialized({:incremental, strategy: :delete_insert, window_column: :bucket_hour})
  execution_pool(:duckdb)

  meta(tags: [:hourly])

  contract do
    grain(by: [:bucket_hour, :stage], description: "one stage total for one hour")

    column(:bucket_hour, :datetime, null: false)
    column(:stage, :string, null: false)
    column(:deal_count, :integer, null: false)
    column(:pipeline_amount_cents, :integer, null: false)
  end

  check :no_negative_amounts,
    at: :before_materialize,
    on_violation: :fail,
    message: "an hour's pipeline total cannot be negative" do
    ~SQL"""
    select
      count(*) filter (where pipeline_amount_cents < 0) = 0 as passed,
      count(*) filter (where pipeline_amount_cents < 0) as invalid_rows
    from query()
    """
  end

  query(file: "stage_hourly.sql")
end
