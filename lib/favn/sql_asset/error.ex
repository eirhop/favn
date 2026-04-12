defmodule Favn.SQLAsset.Error do
  @moduledoc """
  Normalized SQL asset runtime/render error.
  """

  alias Favn.SQL.Template.Span

  @enforce_keys [:type, :phase, :message]
  defstruct [
    :type,
    :phase,
    :message,
    :asset_ref,
    :span,
    :file,
    :line,
    stack: [],
    details: %{},
    cause: nil
  ]

  @type type ::
          :invalid_asset_input
          | :not_sql_asset
          | :invalid_sql_asset_definition
          | :missing_runtime_input
          | :missing_query_param
          | :unresolved_asset_ref
          | :invalid_produced_relation
          | :cross_connection_asset_ref
          | :defsql_expansion_failed
          | :binding_failure
          | :materialization_planning_failed
          | :backend_execution_failed
          | :unsupported_materialization

  @type phase :: :render | :preview | :explain | :materialize | :runtime

  @type t :: %__MODULE__{
          type: type(),
          phase: phase(),
          message: String.t(),
          asset_ref: Favn.asset_ref() | nil,
          span: Span.t() | nil,
          file: String.t() | nil,
          line: pos_integer() | nil,
          stack: [map()],
          details: map(),
          cause: term()
        }
end
