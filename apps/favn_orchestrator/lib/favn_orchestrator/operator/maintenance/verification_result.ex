defmodule FavnOrchestrator.Operator.Maintenance.VerificationResult do
  @moduledoc """
  Operator-facing result for backup verification.
  """

  @type t :: %__MODULE__{
          operation: :verify_backup,
          operation_status: :ok,
          adapter: atom(),
          backup_status: atom(),
          integrity_check_status: atom() | nil,
          schema_status: atom() | nil,
          checksum: String.t() | nil,
          byte_size: non_neg_integer() | nil,
          failure_category: atom() | nil,
          warnings: [term()]
        }

  defstruct operation: :verify_backup,
            operation_status: :ok,
            adapter: :unknown,
            backup_status: :unknown,
            integrity_check_status: nil,
            schema_status: nil,
            checksum: nil,
            byte_size: nil,
            failure_category: nil,
            warnings: []

  @doc "Normalizes adapter-owned verification maps into a stable DTO."
  @spec new(map() | keyword()) :: t()
  def new(attrs) do
    attrs = attrs_to_map(attrs)

    %__MODULE__{
      adapter: Map.get(attrs, :adapter, :unknown),
      backup_status: Map.get(attrs, :backup_status, :unknown),
      integrity_check_status: Map.get(attrs, :integrity_check_status),
      schema_status: Map.get(attrs, :schema_status),
      checksum: Map.get(attrs, :checksum),
      byte_size: Map.get(attrs, :byte_size),
      failure_category: Map.get(attrs, :failure_category),
      warnings: Map.get(attrs, :warnings, [])
    }
  end

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
