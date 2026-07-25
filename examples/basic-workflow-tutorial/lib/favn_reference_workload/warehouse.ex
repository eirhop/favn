defmodule FavnReferenceWorkload.Warehouse do
  @moduledoc """
  Shared DuckDB namespace defaults for all CRM workload assets.

  This module groups all warehouse-related assets under a common namespace and
  sets the default connection for descendants.

  Alternative:

  - You can skip this parent namespace module and keep only layer modules.
  - Keeping this module helps readers navigate the project structure.
  """

  use Favn.Namespace

  relation(connection: :warehouse)
  meta(owner: "reference-workload")
end
