defmodule FavnOrchestrator.Operator.Maintenance.BackupResult do
  @moduledoc """
  Operator-facing result for SQLite control-plane backup creation.
  """

  alias FavnOrchestrator.Operator.Maintenance.VerificationResult

  @type t :: %__MODULE__{
          operation: :backup,
          operation_status: :ok,
          adapter: atom(),
          destination_identity: map(),
          byte_size: non_neg_integer() | nil,
          checksum: String.t() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer(),
          checkpoint_policy: atom() | nil,
          verification: VerificationResult.t() | nil,
          warnings: [term()]
        }

  defstruct operation: :backup,
            operation_status: :ok,
            adapter: :unknown,
            destination_identity: %{path: :redacted},
            byte_size: nil,
            checksum: nil,
            started_at: nil,
            finished_at: nil,
            duration_ms: 0,
            checkpoint_policy: nil,
            verification: nil,
            warnings: []

  @doc "Normalizes adapter-owned backup maps into a stable DTO."
  @spec new(map() | keyword()) :: t()
  def new(attrs) do
    attrs = attrs_to_map(attrs)

    %__MODULE__{
      adapter: Map.get(attrs, :adapter, :unknown),
      destination_identity: Map.get(attrs, :destination_identity, %{path: :redacted}),
      byte_size: Map.get(attrs, :byte_size),
      checksum: Map.get(attrs, :checksum),
      started_at: Map.get(attrs, :started_at),
      finished_at: Map.get(attrs, :finished_at),
      duration_ms: Map.get(attrs, :duration_ms, 0),
      checkpoint_policy: Map.get(attrs, :checkpoint_policy),
      verification: normalize_verification(Map.get(attrs, :verification)),
      warnings: Map.get(attrs, :warnings, [])
    }
  end

  defp normalize_verification(nil), do: nil
  defp normalize_verification(%VerificationResult{} = result), do: result
  defp normalize_verification(result) when is_map(result), do: VerificationResult.new(result)

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
