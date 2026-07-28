defmodule CrmDemo.Warehouse.Core do
  @moduledoc """
  Reusable models that no longer mention a source system.

  Core reads persisted Source or Core relations - never landing files, never an
  API. This is the boundary that lets a second CRM be added later without
  rewriting the marts: only Source and the Core join change.
  """

  use Favn.Namespace

  relation(schema: "core")
  meta(tags: [:core])
end
