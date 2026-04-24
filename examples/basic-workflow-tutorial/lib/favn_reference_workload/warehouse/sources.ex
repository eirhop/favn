defmodule FavnReferenceWorkload.Warehouse.Sources do
  @moduledoc """
  Namespace defaults for source relations.

  This module sets `schema: "sources"` once so source modules do not need to
  repeat that setting.

  Alternative:

  - Remove this module and set `schema: "sources"` directly in each source
    asset module.
  """

  use Favn.Namespace, relation: [connection: :warehouse, schema: "sources"]
end
