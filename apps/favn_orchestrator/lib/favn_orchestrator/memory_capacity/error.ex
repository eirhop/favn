defmodule FavnOrchestrator.MemoryCapacity.Error do
  @moduledoc """
  Stable memory-admission failure returned before manifest-heavy work allocates.
  """

  @enforce_keys [:code, :required_bytes]
  defstruct [:code, :required_bytes, :available_bytes, :limit_bytes, :usage_bytes]

  @type code ::
          :manifest_capacity_busy
          | :manifest_capacity_unavailable
          | :memory_capacity_unknown

  @type t :: %__MODULE__{
          code: code(),
          required_bytes: non_neg_integer(),
          available_bytes: non_neg_integer() | nil,
          limit_bytes: pos_integer() | nil,
          usage_bytes: non_neg_integer() | nil
        }
end
