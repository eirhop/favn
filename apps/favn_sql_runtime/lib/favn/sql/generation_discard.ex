defmodule Favn.SQL.GenerationDiscard do
  @moduledoc """
  Identity-bearing request to discard a non-active physical generation.

  The adapter reads the logical target marker before dropping the candidate so
  a committed activation with a lost reply cannot delete the active relation.
  """

  alias Favn.RelationRef

  @enforce_keys [
    :logical_target_id,
    :stable_relation,
    :candidate_generation_id,
    :candidate_relation
  ]
  defstruct [
    :logical_target_id,
    :stable_relation,
    :candidate_generation_id,
    :candidate_relation
  ]

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          stable_relation: RelationRef.t(),
          candidate_generation_id: String.t(),
          candidate_relation: RelationRef.t()
        }
end
