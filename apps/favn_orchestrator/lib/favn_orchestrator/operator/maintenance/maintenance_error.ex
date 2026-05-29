defmodule FavnOrchestrator.Operator.Maintenance.MaintenanceError do
  @moduledoc """
  Stable operator-facing maintenance error.

  Error values are safe for local operator output, logs, and telemetry. Untrusted
  reasons and details are redacted at construction time.
  """

  alias FavnOrchestrator.Redaction

  @categories [
    :invalid_configuration,
    :unsupported_adapter,
    :database_unavailable,
    :schema_not_ready,
    :migration_not_allowed,
    :migration_failed,
    :backup_failed,
    :backup_invalid,
    :verification_failed,
    :filesystem_error
  ]

  @type category ::
          :invalid_configuration
          | :unsupported_adapter
          | :database_unavailable
          | :schema_not_ready
          | :migration_not_allowed
          | :migration_failed
          | :backup_failed
          | :backup_invalid
          | :verification_failed
          | :filesystem_error

  @type t :: %__MODULE__{
          category: category(),
          operation: atom(),
          adapter: atom() | nil,
          reason: term(),
          retryable?: boolean(),
          details: map()
        }

  defstruct category: :invalid_configuration,
            operation: :unknown,
            adapter: nil,
            reason: nil,
            retryable?: false,
            details: %{}

  @doc "Builds a redacted maintenance error."
  @spec new(atom(), category(), term(), keyword() | map()) :: t()
  def new(operation, category, reason, attrs \\ [])
      when is_atom(operation) and category in @categories do
    attrs = attrs_to_map(attrs)

    %__MODULE__{
      category: category,
      operation: operation,
      adapter: Map.get(attrs, :adapter),
      reason: Redaction.redact_operational(reason),
      retryable?: Map.get(attrs, :retryable?, false),
      details: Redaction.redact_operational(Map.get(attrs, :details, %{}))
    }
  end

  @doc "Normalizes adapter-owned error maps into maintenance errors."
  @spec normalize(atom(), term(), keyword() | map()) :: t()
  def normalize(operation, reason, attrs \\ [])

  def normalize(operation, %__MODULE__{} = error, attrs) do
    attrs = attrs_to_map(attrs)
    %{error | operation: operation, adapter: error.adapter || Map.get(attrs, :adapter)}
  end

  def normalize(operation, %{category: category} = error, attrs) when category in @categories do
    attrs = attrs_to_map(attrs)

    new(operation, category, Map.get(error, :reason),
      adapter: Map.get(error, :adapter, Map.get(attrs, :adapter)),
      retryable?: Map.get(error, :retryable?, false),
      details: Map.get(error, :details, %{})
    )
  end

  def normalize(operation, reason, attrs) do
    attrs = attrs_to_map(attrs)
    new(operation, Map.get(attrs, :category, :invalid_configuration), reason, attrs)
  end

  defp attrs_to_map(attrs) when is_map(attrs), do: attrs
  defp attrs_to_map(attrs) when is_list(attrs), do: Map.new(attrs)
end
