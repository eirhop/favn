defmodule Favn.TargetCompatibility do
  @moduledoc """
  Pure desired/active/physical compatibility classification for SQL targets.

  The classifier does not inspect databases or persist decisions. Callers pass
  validated manifest descriptors plus the current and recorded physical
  fingerprints, then persist the returned bounded decision at the orchestrator
  boundary.
  """

  alias Favn.Manifest.TargetDescriptor
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TargetCompatibility.Result

  @structural_fields [
    :relation,
    :adapter,
    :connection_identity,
    :materialization,
    :write_semantics,
    :contract_fingerprint,
    :grain_fingerprint,
    :window_identity
  ]

  @doc """
  Classifies one desired target against its active and observed state.

  Passing `nil` as the active descriptor means there is no bound active
  generation. `:not_found` means physical inspection authoritatively found no
  relation; inspection failures must be handled before calling this function.
  """
  @spec classify(
          TargetDescriptor.t(),
          TargetDescriptor.t() | nil,
          String.t() | nil,
          PhysicalFingerprint.t() | :not_found
        ) :: Result.t()
  def classify(
        %TargetDescriptor{} = _desired,
        nil,
        nil,
        :not_found
      ) do
    result(:uninitialized, :no_active_generation)
  end

  def classify(
        %TargetDescriptor{} = _desired,
        nil,
        nil,
        %PhysicalFingerprint{} = observed
      ) do
    result(:operator_decision, :unmanaged_physical_relation, %{
      physical: physical_diff(nil, observed)
    })
  end

  def classify(%TargetDescriptor{} = _desired, nil, recorded, observed) do
    result(:operator_decision, :inconsistent_generation_state, %{
      physical: physical_diff(recorded, observed)
    })
  end

  def classify(
        %TargetDescriptor{} = _desired,
        %TargetDescriptor{} = _active,
        recorded,
        :not_found
      ) do
    result(:unexpected_drift, :physical_relation_missing, %{
      physical: physical_diff(recorded, :not_found)
    })
  end

  def classify(
        %TargetDescriptor{} = _desired,
        %TargetDescriptor{} = _active,
        nil,
        %PhysicalFingerprint{} = observed
      ) do
    result(:operator_decision, :active_physical_fingerprint_missing, %{
      physical: physical_diff(nil, observed)
    })
  end

  def classify(
        %TargetDescriptor{} = _desired,
        %TargetDescriptor{} = _active,
        recorded,
        %PhysicalFingerprint{fingerprint: observed}
      )
      when is_binary(recorded) and recorded != observed do
    result(:unexpected_drift, :physical_fingerprint_mismatch, %{
      physical: %{recorded_fingerprint: recorded, observed_fingerprint: observed}
    })
  end

  def classify(
        %TargetDescriptor{} = desired,
        %TargetDescriptor{} = active,
        fingerprint,
        %PhysicalFingerprint{fingerprint: fingerprint} = physical
      ) do
    case classify_descriptors(desired, active) do
      %Result{status: :rebuild_required} = result ->
        result

      %Result{} = result ->
        case PhysicalFingerprint.identity_diff(desired, physical) do
          [] ->
            result

          diff ->
            result(:unexpected_drift, :physical_identity_mismatch, %{
              physical_identity: diff
            })
        end
    end
  end

  @doc "Returns the named desired-descriptor fields that require a rebuild when changed."
  @spec structural_fields() :: [atom()]
  def structural_fields, do: @structural_fields

  defp classify_descriptors(desired, active) do
    structural_diff = field_diff(desired, active, @structural_fields)

    cond do
      structural_diff != [] ->
        result(:rebuild_required, :incompatible_descriptor, %{
          descriptor: structural_diff
        })

      desired.execution_package_hash != active.execution_package_hash ->
        result(:rebuild_available, :transformation_changed, %{
          descriptor: field_diff(desired, active, [:execution_package_hash])
        })

      true ->
        result(:ready, :compatible)
    end
  end

  defp field_diff(desired, active, fields) do
    for field <- fields,
        active_value = Map.fetch!(active, field),
        desired_value = Map.fetch!(desired, field),
        active_value != desired_value do
      %{field: field, active: active_value, desired: desired_value}
    end
  end

  defp physical_diff(recorded, :not_found),
    do: %{recorded_fingerprint: recorded, observed_fingerprint: nil, relation: nil}

  defp physical_diff(recorded, %PhysicalFingerprint{} = observed),
    do: %{
      recorded_fingerprint: recorded,
      observed_fingerprint: observed.fingerprint,
      relation: observed.relation
    }

  defp result(status, reason_code, diff \\ %{}),
    do: %Result{status: status, reason_code: reason_code, diff: diff}
end
