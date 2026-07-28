defmodule CrmDemo.Warehouse.Mart.Sales.ExecutiveOverview do
  @moduledoc """
  A view that joins the two marts into one compact daily summary.

  `materialized(:view)` overrides the `:table` default inherited from
  `CrmDemo.Warehouse`. A view has no candidate to stage, so it declares no
  contract.
  """

  use Favn.SQLAsset

  alias CrmDemo.Warehouse.Mart.Sales.{AccountHealth, PipelineDaily}

  depends(AccountHealth)
  depends(PipelineDaily)
  relation(name: "executive_overview")
  materialized(:view)
  execution_pool(:duckdb)

  query(file: "executive_overview.sql")
end
