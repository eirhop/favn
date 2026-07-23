defmodule Favn.SQL.GenerationMarkerInitialization do
  @moduledoc "Initial-generation sidecar marker initialization request."

  alias Favn.RelationRef

  @enforce_keys [
    :logical_target_id,
    :stable_relation,
    :active_generation_id,
    :expected_physical_fingerprint,
    :initialization_operation_id,
    :initialization_token,
    :initialized_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          stable_relation: RelationRef.t(),
          active_generation_id: String.t(),
          expected_physical_fingerprint: String.t(),
          initialization_operation_id: String.t(),
          initialization_token: String.t(),
          initialized_at: DateTime.t()
        }
end
