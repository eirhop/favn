defmodule Favn.SQL.GenerationCapabilities do
  @moduledoc """
  Explicit data-plane capabilities required by target-generation rebuilds.

  These capabilities are deliberately separate from `Favn.SQL.Capabilities`.
  A backend that supports transactions or table replacement does not thereby
  promise safe candidate isolation, marker reconciliation, or atomic activation.
  """

  @type support :: :supported | :unsupported

  @type t :: %__MODULE__{
          transactional_ddl: support(),
          isolated_candidates: support(),
          physical_inspection: support(),
          atomic_swap: support(),
          marker_reconciliation: support(),
          idempotent_discard: support(),
          snapshots: support(),
          max_identifier_bytes: pos_integer()
        }

  defstruct transactional_ddl: :unsupported,
            isolated_candidates: :unsupported,
            physical_inspection: :unsupported,
            atomic_swap: :unsupported,
            marker_reconciliation: :unsupported,
            idempotent_discard: :unsupported,
            snapshots: :unsupported,
            max_identifier_bytes: 128

  @required_rebuild_capabilities [
    :transactional_ddl,
    :isolated_candidates,
    :physical_inspection,
    :atomic_swap,
    :marker_reconciliation,
    :idempotent_discard
  ]

  @doc "Returns whether every capability required for a safe rebuild is supported."
  @spec rebuild_supported?(t()) :: boolean()
  def rebuild_supported?(%__MODULE__{} = capabilities) do
    Enum.all?(@required_rebuild_capabilities, &(Map.fetch!(capabilities, &1) == :supported))
  end

  @doc "Returns the required rebuild capabilities that are not supported."
  @spec missing_for_rebuild(t()) :: [atom()]
  def missing_for_rebuild(%__MODULE__{} = capabilities) do
    Enum.reject(@required_rebuild_capabilities, &(Map.fetch!(capabilities, &1) == :supported))
  end
end
