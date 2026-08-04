defmodule CrmDemo.Warehouse.Mart.Sales.StageYearly do
  @moduledoc """
  Deal count and value per stage per calendar year.

  The coarsest grain Favn offers. Every year coverage expects fits on one screen,
  which is why a year-grained asset has nothing above it to page through.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Core.Sales.Events.Opportunity

  depends(Opportunity)
  relation(name: "stage_yearly")
  window(Favn.Window.yearly(timezone: "Etc/UTC", required: true))
  coverage(from: ~D[2022-01-01], through: :latest_closed, availability_delay: {:days, 7})
  freshness(window_success: true)
  materialized({:incremental, strategy: :delete_insert, window_column: :bucket_year})
  execution_pool(:duckdb)

  meta(tags: [:yearly])

  contract do
    grain(by: [:bucket_year, :stage], description: "one stage total for one year")

    column(:bucket_year, :date, null: false)
    column(:stage, :string, null: false)
    column(:deal_count, :integer, null: false)
    column(:pipeline_amount_cents, :integer, null: false)
  end

  query(file: "stage_yearly.sql")
end
