defmodule Favn.SQL.GenerationReconciliation do
  @moduledoc """
  Read-only request for the marker associated with a logical target.
  """

  alias Favn.RelationRef

  @enforce_keys [:logical_target_id, :stable_relation]
  defstruct [:logical_target_id, :stable_relation]

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          stable_relation: RelationRef.t()
        }
end
