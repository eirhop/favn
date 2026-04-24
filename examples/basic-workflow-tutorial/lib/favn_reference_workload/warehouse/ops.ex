defmodule FavnReferenceWorkload.Warehouse.Ops do
  @moduledoc """
  Namespace defaults for ops/control assets.

  This sets `schema: "ops"` for child modules.

  Ops assets are useful for final markers, monitoring hooks, and workflow
  control signals.
  """

  use Favn.Namespace, relation: [connection: :warehouse, schema: "ops"]
end
