defmodule FavnReferenceWorkload.Warehouse.Core do
  @moduledoc "System-agnostic CRM business models."

  use Favn.Namespace

  relation(schema: "core")
  materialized(:table)
  meta(owner: "generic-crm-demo", tags: [:core, :model])
end
