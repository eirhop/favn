defmodule Favn.Contracts.RelationInspectionRequest do
  @moduledoc """
  Runner-owned read-only relation inspection request.
  """

  alias Favn.RelationRef

  @type include_item :: :relation | :columns | :row_count | :sample | :table_metadata

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          manifest_content_hash: String.t() | nil,
          asset_ref: Favn.Ref.t() | nil,
          relation: RelationRef.t() | nil,
          include: [include_item()],
          sample_limit: non_neg_integer()
        }

  defstruct manifest_version_id: nil,
            manifest_content_hash: nil,
            asset_ref: nil,
            relation: nil,
            include: [:relation, :columns, :row_count, :sample, :table_metadata],
            sample_limit: 20
end
