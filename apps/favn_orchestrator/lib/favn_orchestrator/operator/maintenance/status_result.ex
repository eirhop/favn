defmodule FavnOrchestrator.Operator.Maintenance.StatusResult do
  @moduledoc """
  Operator-facing SQLite control-plane maintenance status.
  """

  @type t :: %__MODULE__{
          operation: :status,
          operation_status: :ok,
          adapter: atom(),
          migration_mode: :auto | :manual | nil,
          readiness_status: atom() | nil,
          ready?: boolean(),
          schema_status: atom() | nil,
          missing_versions: [String.t()],
          future_versions: [String.t()],
          missing_tables: [String.t()],
          database_identity: map(),
          warnings: [term()]
        }

  defstruct operation: :status,
            operation_status: :ok,
            adapter: :unknown,
            migration_mode: nil,
            readiness_status: nil,
            ready?: false,
            schema_status: nil,
            missing_versions: [],
            future_versions: [],
            missing_tables: [],
            database_identity: %{path: :redacted},
            warnings: []

  @doc "Normalizes adapter-owned status maps into a stable DTO."
  @spec new(map() | keyword()) :: t()
  def new(attrs) do
    attrs = attrs_to_map(attrs)
    schema = Map.get(attrs, :schema, %{}) || %{}

    %__MODULE__{
      adapter: Map.get(attrs, :adapter, :unknown),
      migration_mode: Map.get(attrs, :migration_mode),
      readiness_status: Map.get(attrs, :readiness_status, Map.get(attrs, :status)),
      ready?: Map.get(attrs, :ready?, false),
      schema_status: Map.get(attrs, :schema_status, Map.get(schema, :status)),
      missing_versions: Map.get(attrs, :missing_versions, Map.get(schema, :missing_versions, [])),
      future_versions: Map.get(attrs, :future_versions, Map.get(schema, :future_versions, [])),
      missing_tables: Map.get(attrs, :missing_tables, Map.get(schema, :missing_tables, [])),
      database_identity:
        Map.get(attrs, :database_identity, Map.get(attrs, :database, %{path: :redacted})),
      warnings: Map.get(attrs, :warnings, [])
    }
  end

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
