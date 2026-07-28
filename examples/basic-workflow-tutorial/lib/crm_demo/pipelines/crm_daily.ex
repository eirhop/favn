defmodule CrmDemo.Pipelines.CrmDaily do
  @moduledoc """
  One UTC day of CRM events, from the API through to the daily marts.

  `window(:daily, ...)` gives runs an anchor: with no `--window` the pipeline
  selects the latest complete day, and a schedule occurrence selects the day
  before it. Selecting `Engagement` alongside `ExecutiveOverview` keeps the
  engagement branch in the run even though no mart reads it yet.
  """

  use Favn.Pipeline

  alias CrmDemo.Warehouse.Core.Sales.Events.Engagement
  alias CrmDemo.Warehouse.Mart.Sales.ExecutiveOverview

  pipeline :crm_daily do
    assets([Engagement, ExecutiveOverview])
    deps(:all)
    window(:daily, anchor: :previous_complete_period, timezone: "Etc/UTC")
    schedule(cron: "0 2 * * *", timezone: "Etc/UTC", missed: :one, overlap: :forbid)
    max_concurrency(2)
    execution_pool(:duckdb)
  end
end
