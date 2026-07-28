defmodule Favn.SQL.GenerationReconciliation do
  @moduledoc """
  Read-only request for the marker associated with a logical target.

  `require_relation_instance?` defaults to `true`, so a marker is accepted only
  when its token is bound to the inspected physical table. Favn's authorized
  managed rebuild and discard paths may set it to `false` solely to replace or
  clean up an unbound legacy relation; callers must not use that opt-out to
  adopt a physical relation or resolve an unknown outcome.
  """

  alias Favn.RelationRef

  @enforce_keys [:logical_target_id, :stable_relation]
  defstruct [:logical_target_id, :stable_relation, require_relation_instance?: true]

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          stable_relation: RelationRef.t(),
          require_relation_instance?: boolean()
        }
end
