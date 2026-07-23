defmodule Favn.TargetCompatibility.Result do
  @moduledoc """
  Pure compatibility decision for one persisted SQL target.

  `diff` is a bounded, JSON-friendly explanation of the fields that produced
  the decision. Orchestrator persistence may store this value directly after
  converting the atom reason code to its string representation.
  """

  @type status ::
          :ready
          | :uninitialized
          | :rebuild_available
          | :rebuild_required
          | :unexpected_drift
          | :operator_decision

  @type reason_code ::
          :compatible
          | :no_active_generation
          | :transformation_changed
          | :incompatible_descriptor
          | :physical_fingerprint_mismatch
          | :physical_identity_mismatch
          | :physical_relation_missing
          | :unmanaged_physical_relation
          | :active_physical_fingerprint_missing
          | :inconsistent_generation_state

  @type t :: %__MODULE__{
          status: status(),
          reason_code: reason_code(),
          diff: map()
        }

  @enforce_keys [:status, :reason_code, :diff]
  defstruct [:status, :reason_code, :diff]

  @doc "Returns whether ordinary materialization is allowed by this decision."
  @spec writable?(t()) :: boolean()
  def writable?(%__MODULE__{status: status}),
    do: status in [:ready, :uninitialized, :rebuild_available]
end
