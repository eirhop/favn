defmodule FavnOrchestrator.ManifestStore.Lease do
  @moduledoc false

  alias Favn.Manifest.Version
  alias FavnOrchestrator.MemoryCapacity

  @enforce_keys [:version, :capacity_token, :reserved_bytes]
  defstruct [:version, :capacity_token, :reserved_bytes]

  @type t :: %__MODULE__{
          version: Version.t(),
          capacity_token: MemoryCapacity.t(),
          reserved_bytes: non_neg_integer()
        }
end
