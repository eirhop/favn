defmodule FavnReferenceWorkload.Warehouse.Gold do
  @moduledoc """
  Namespace defaults for gold-layer assets.

  This sets `schema: "gold"` for child modules.

  Gold assets are business-facing outputs, such as KPIs and summaries used by
  analysts and stakeholders.
  """

  use Favn.Namespace, relation: [connection: :warehouse, schema: "gold"]
end
