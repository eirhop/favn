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
          | :invalid_relation
          | :cross_connection_asset_ref
          | :defsql_expansion_failed
          | :binding_failure
          | :unresolved_runtime_relation
          | :invalid_check_result
          | :check_failed
          | :materialization_planning_failed
          | :backend_execution_failed
          | :runtime_inputs_missing_module
          | :runtime_inputs_missing_callback
          | :runtime_inputs_raised
          | :runtime_inputs_invalid_result
          | :runtime_inputs_timeout
          | :runtime_inputs_cancelled
          | :runtime_inputs_failed
          | :runtime_inputs_param_collision
          | :runtime_inputs_payload_too_large
          | :unsupported_materialization

  @type phase ::
          :render
          | :preview
          | :explain
          | :before_materialize
          | :materialize
          | :after_materialize
          | :runtime_inputs
          | :runtime

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
