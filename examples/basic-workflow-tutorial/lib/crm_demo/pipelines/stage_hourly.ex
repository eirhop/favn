defmodule CrmDemo.Pipelines.StageHourly do
  @moduledoc """
  The hour-grained stage rollup, on its own hourly anchor.

  A pipeline carries one window policy and a run context is what tells an asset
  which anchor to use, so an asset per grain needs a pipeline per grain.

  `deps(:none)` because the Core model this reads is already materialized by
  `CrmDemo.Pipelines.CrmDaily`. Pulling it in would re-run a day-grained graph
  under an hour-grained anchor to no purpose.

  Unscheduled on purpose: it exists to be run by hand while looking at the screen
  that shows it.
  """

  use Favn.Pipeline

  alias CrmDemo.Warehouse.Mart.Sales.StageHourly

  pipeline :stage_hourly do
    assets([StageHourly])
    deps(:none)
    window(:hourly, anchor: :previous_complete_period, timezone: "Etc/UTC")
    max_concurrency(2)
    execution_pool(:duckdb)
  end
end
