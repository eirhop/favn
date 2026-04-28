defmodule Favn.Contracts.RelationInspectionResult do
  @moduledoc """
  JSON-friendly result for safe read-only relation inspection.
  """

  alias Favn.RelationRef

  @type warning :: %{required(:code) => atom(), optional(:message) => String.t()}

  @type t :: %__MODULE__{
          asset_ref: Favn.Ref.t() | nil,
          relation_ref: RelationRef.t() | nil,
          relation: term() | nil,
          columns: [term()],
          row_count: non_neg_integer() | nil,
          sample: map() | nil,
          table_metadata: map(),
          adapter: atom() | nil,
          inspected_at: DateTime.t(),
          warnings: [warning()],
          error: map() | nil
        }

  defstruct asset_ref: nil,
            relation_ref: nil,
            relation: nil,
            columns: [],
            row_count: nil,
            sample: nil,
            table_metadata: %{},
            adapter: nil,
            inspected_at: nil,
            warnings: [],
            error: nil
end
