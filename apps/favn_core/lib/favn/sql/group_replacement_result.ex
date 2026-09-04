defmodule Favn.SQL.GroupReplacementResult do
  @moduledoc """
  Bounded outcome details for one atomic group-scoped replacement.

  This is separate from checked materialization's `write_outcome`, which
  remains `:written | :no_op`.
  """

  @type operation ::
          :replaced
          | :delete_only
          | :empty_scope_no_op
          | :before_check_skipped
          | :bootstrap_created
          | :bootstrap_empty

  @enforce_keys [
    :operation,
    :scope_group_count,
    :candidate_row_count,
    :inserted_row_count,
    :deleted_row_count
  ]
  defstruct [
    :operation,
    :scope_group_count,
    :candidate_row_count,
    :inserted_row_count,
    :deleted_row_count
  ]

  @type t :: %__MODULE__{
          operation: operation(),
          scope_group_count: non_neg_integer(),
          candidate_row_count: non_neg_integer(),
          inserted_row_count: non_neg_integer(),
          deleted_row_count: non_neg_integer() | :unavailable
        }
end
