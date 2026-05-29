defmodule FavnOrchestrator.Operator.Maintenance.MigrationResult do
  @moduledoc """
  Operator-facing result for explicit control-plane schema migration.
  """

  @type t :: %__MODULE__{
          operation: :migrate,
          operation_status: :ok,
          adapter: atom(),
          action: :dry_run | :migrated | :noop,
          dry_run?: boolean(),
          previous_schema_status: atom() | nil,
          final_schema_status: atom() | nil,
          migrated_versions: [String.t()],
          migrated_count: non_neg_integer(),
          duration_ms: non_neg_integer(),
          warnings: [term()]
        }

  defstruct operation: :migrate,
            operation_status: :ok,
            adapter: :unknown,
            action: :noop,
            dry_run?: false,
            previous_schema_status: nil,
            final_schema_status: nil,
            migrated_versions: [],
            migrated_count: 0,
            duration_ms: 0,
            warnings: []

  @doc "Normalizes adapter-owned migration maps into a stable DTO."
  @spec new(map() | keyword()) :: t()
  def new(attrs) do
    attrs = attrs_to_map(attrs)
    migrated_versions = Map.get(attrs, :migrated_versions, [])

    %__MODULE__{
      adapter: Map.get(attrs, :adapter, :unknown),
      action: Map.get(attrs, :action, :noop),
      dry_run?: Map.get(attrs, :dry_run?, false),
      previous_schema_status: Map.get(attrs, :previous_schema_status),
      final_schema_status: Map.get(attrs, :final_schema_status),
      migrated_versions: migrated_versions,
      migrated_count: Map.get(attrs, :migrated_count, length(migrated_versions)),
      duration_ms: Map.get(attrs, :duration_ms, 0),
      warnings: Map.get(attrs, :warnings, [])
    }
  end

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
