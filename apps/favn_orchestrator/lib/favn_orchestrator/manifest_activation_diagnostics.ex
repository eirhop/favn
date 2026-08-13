defmodule FavnOrchestrator.ManifestActivationDiagnostics do
  @moduledoc """
  Bounded operator diagnostics returned with one manifest activation.

  Compatibility decisions remain durable target-binding state. This value only
  summarizes inspection failures that require an operator to repeat activation
  after correcting a transient runner or data-system problem.
  """

  alias FavnOrchestrator.Persistence.Commands.DeploymentTargetCompatibility

  @max_unresolved_inspections 20

  @enforce_keys [
    :unresolved_inspection_count,
    :unresolved_inspections,
    :truncated?,
    :recovery
  ]
  defstruct unresolved_inspection_count: 0,
            unresolved_inspections: [],
            truncated?: false,
            recovery: nil

  @type unresolved_inspection :: %{
          required(:target_id) => String.t(),
          required(:reason_code) => String.t()
        }

  @type recovery :: %{
          required(:action) => :repeat_manifest_activation,
          required(:requires_new_idempotency_key) => true,
          required(:message) => String.t()
        }

  @type t :: %__MODULE__{
          unresolved_inspection_count: non_neg_integer(),
          unresolved_inspections: [unresolved_inspection()],
          truncated?: boolean(),
          recovery: recovery() | nil
        }

  @doc "Summarizes unresolved physical inspections without exposing unbounded details."
  @spec from_compatibilities([DeploymentTargetCompatibility.t()]) :: t()
  def from_compatibilities(compatibilities) when is_list(compatibilities) do
    unresolved =
      compatibilities
      |> Enum.filter(&unresolved_inspection?/1)
      |> Enum.map(&inspection_summary/1)
      |> Enum.sort_by(& &1.target_id)

    count = length(unresolved)

    %__MODULE__{
      unresolved_inspection_count: count,
      unresolved_inspections: Enum.take(unresolved, @max_unresolved_inspections),
      truncated?: count > @max_unresolved_inspections,
      recovery: recovery(count)
    }
  end

  @doc "Returns the stable JSON-facing diagnostics shape."
  @spec to_map(t() | nil) :: map()
  def to_map(%__MODULE__{} = diagnostics) do
    %{
      unresolved_inspection_count: diagnostics.unresolved_inspection_count,
      unresolved_inspections: diagnostics.unresolved_inspections,
      truncated: diagnostics.truncated?,
      recovery: diagnostics.recovery
    }
  end

  def to_map(nil), do: to_map(from_compatibilities([]))

  @doc "Restores and validates diagnostics from a durable command receipt."
  @spec from_map(map() | nil) :: {:ok, t() | nil} | {:error, :invalid_activation_diagnostics}
  def from_map(nil), do: {:ok, nil}

  def from_map(diagnostics) when is_map(diagnostics) do
    count = field(diagnostics, :unresolved_inspection_count)
    unresolved = field(diagnostics, :unresolved_inspections)
    truncated? = field(diagnostics, :truncated)
    recovery = field(diagnostics, :recovery)

    if is_integer(count) and count >= 0 and is_list(unresolved) and
         valid_bounded_summary?(count, unresolved, truncated?) and
         Enum.all?(unresolved, &valid_inspection_summary?/1) and
         valid_recovery?(recovery, count) do
      {:ok,
       %__MODULE__{
         unresolved_inspection_count: count,
         unresolved_inspections: Enum.map(unresolved, &normalize_inspection_summary/1),
         truncated?: truncated?,
         recovery: normalize_recovery(recovery)
       }}
    else
      {:error, :invalid_activation_diagnostics}
    end
  end

  def from_map(_diagnostics), do: {:error, :invalid_activation_diagnostics}

  defp unresolved_inspection?(%DeploymentTargetCompatibility{
         compatibility_status: :operator_decision,
         reason_code: "physical_inspection_unavailable"
       }),
       do: true

  defp unresolved_inspection?(_compatibility), do: false

  defp inspection_summary(%DeploymentTargetCompatibility{} = compatibility) do
    %{
      target_id: compatibility.target_id,
      reason_code: compatibility.reason_code
    }
  end

  defp recovery(0), do: nil

  defp recovery(_count) do
    %{
      action: :repeat_manifest_activation,
      requires_new_idempotency_key: true,
      message:
        "Correct the runner or data-system problem, then repeat manifest activation with a new idempotency key."
    }
  end

  defp valid_inspection_summary?(summary) when is_map(summary) do
    target_id = field(summary, :target_id)
    reason_code = field(summary, :reason_code)

    is_binary(target_id) and target_id != "" and byte_size(target_id) <= 512 and
      reason_code == "physical_inspection_unavailable"
  end

  defp valid_inspection_summary?(_summary), do: false

  defp valid_bounded_summary?(count, unresolved, false), do: count == length(unresolved)

  defp valid_bounded_summary?(count, unresolved, true),
    do: length(unresolved) == @max_unresolved_inspections and count > length(unresolved)

  defp valid_bounded_summary?(_count, _unresolved, _truncated?), do: false

  defp valid_recovery?(nil, 0), do: true

  defp valid_recovery?(recovery, count) when is_map(recovery) and count > 0 do
    field(recovery, :action) in [:repeat_manifest_activation, "repeat_manifest_activation"] and
      field(recovery, :requires_new_idempotency_key) == true and
      is_binary(field(recovery, :message)) and byte_size(field(recovery, :message)) <= 512
  end

  defp valid_recovery?(_recovery, _count), do: false

  defp normalize_inspection_summary(summary) do
    %{target_id: field(summary, :target_id), reason_code: field(summary, :reason_code)}
  end

  defp normalize_recovery(nil), do: nil

  defp normalize_recovery(recovery) do
    %{
      action: :repeat_manifest_activation,
      requires_new_idempotency_key: true,
      message: field(recovery, :message)
    }
  end

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
