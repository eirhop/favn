defmodule FavnReferenceWorkload.Pipelines.DailyCrmAnalytics do
  @moduledoc "Temporary scheduled daily-window CRM analytics pipeline."

  use Favn.Pipeline

  pipeline :daily_crm_analytics do
    asset(FavnReferenceWorkload.Warehouse.Mart.ExecutiveOverview)
    deps(:all)
    window(:daily, anchor: :previous_complete_period, lookback: 1, timezone: "Etc/UTC")
    schedule(cron: "0 2 * * *", timezone: "Etc/UTC", missed: :one, overlap: :forbid)
    max_concurrency(2)
    execution_pool(:local_landing_write)
    outputs([:pipeline_daily, :executive_overview])
    meta(%{purpose: "daily_window_demo", layer: "mart"})
  end
end
