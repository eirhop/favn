defmodule Favn.SQL.GenerationActivation do
  @moduledoc """
  Atomic target-generation activation request.

  All three relations must share one connection, catalog, and schema. The
  adapter verifies the expected marker before renaming either relation.
  """

  alias Favn.RelationRef
  alias Favn.SQL.GenerationMarker

  @enforce_keys [
    :logical_target_id,
    :stable_relation,
    :candidate_relation,
    :retired_relation,
    :expected_candidate_fingerprint,
    :candidate_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]
  defstruct [
    :logical_target_id,
    :stable_relation,
    :candidate_relation,
    :retired_relation,
    :expected_candidate_fingerprint,
    :expected_active_marker,
    :expected_active_generation_id,
    :candidate_generation_id,
    :activation_operation_id,
    :activation_token,
    :activated_at
  ]

  @type t :: %__MODULE__{
          logical_target_id: String.t(),
          stable_relation: RelationRef.t(),
          candidate_relation: RelationRef.t(),
          retired_relation: RelationRef.t(),
          expected_candidate_fingerprint: String.t(),
          expected_active_marker: GenerationMarker.t() | nil,
          expected_active_generation_id: String.t() | nil,
          candidate_generation_id: String.t(),
          activation_operation_id: String.t(),
          activation_token: String.t(),
          activated_at: DateTime.t()
        }
end
