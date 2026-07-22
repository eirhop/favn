defmodule Favn.SQL.GenerationMarker do
  @moduledoc """
  Observed active-generation marker stored beside a physical SQL target.

  The marker contains no credentials. It is the data-plane authority used to
  reconcile an activation whose commit result was not observed by the runner.
  """

  alias Favn.RelationRef

  @enforce_keys [
    :logical_target_id,
    :active_relation,
    :active_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]
  defstruct [
    :logical_target_id,
    :active_relation,
    :active_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          active_relation: RelationRef.t(),
          active_generation_id: String.t(),
          activation_operation_id: String.t(),
          activation_token: String.t(),
          activated_at: DateTime.t()
        }
end
