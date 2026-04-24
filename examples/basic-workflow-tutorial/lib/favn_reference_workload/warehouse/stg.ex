defmodule FavnReferenceWorkload.Warehouse.Stg do
  @moduledoc """
  Namespace defaults for staging-layer assets.

  This sets `schema: "stg"` for child modules.

  Staging assets usually normalize, join, and clean raw inputs to create
  analytics-friendly tables.
  """

  use Favn.Namespace, relation: [connection: :warehouse, schema: "stg"]
end
