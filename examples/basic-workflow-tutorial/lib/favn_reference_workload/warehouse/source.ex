defmodule FavnReferenceWorkload.Warehouse.Source do
  @moduledoc "DuckDB source relations loaded from the fixed CRM landing files."

  use Favn.Namespace

  relation(schema: "source")
  materialized(:table)
  meta(owner: "generic-crm-demo", tags: [:source, :duckdb])
end
