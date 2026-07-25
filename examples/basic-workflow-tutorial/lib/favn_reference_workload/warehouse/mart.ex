defmodule FavnReferenceWorkload.Warehouse.Mart do
  @moduledoc "Analytics-ready CRM tables for operator and UI inspection."

  use Favn.Namespace

  relation(schema: "mart")
  materialized(:table)
  meta(owner: "generic-crm-demo", tags: [:mart, :analytics])
end
