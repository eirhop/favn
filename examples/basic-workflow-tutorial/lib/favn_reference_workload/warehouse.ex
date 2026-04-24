defmodule FavnReferenceWorkload.Warehouse do
  @moduledoc """
  Shared warehouse namespace defaults for all workload assets.

  This module groups all warehouse-related assets under a common namespace and
  sets the default connection for descendants.

  Alternative:

  - You can skip this parent namespace module and keep only layer modules.
  - Keeping this module helps readers navigate the project structure.
  """

  use Favn.Namespace, relation: [connection: :warehouse]
end
