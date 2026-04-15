defmodule Favn.Asset.Dependency do
  @moduledoc """
  Canonical dependency edge metadata for one asset.
  """

  alias Favn.Asset.RelationInput

  @enforce_keys [:asset_ref, :provenance]
  defstruct [:asset_ref, provenance: [], relation_inputs: []]

  @type provenance :: :explicit | :inferred_sql_relation | :inferred_sql_asset_ref

  @type t :: %__MODULE__{
          asset_ref: Favn.Ref.t(),
          provenance: [provenance()],
          relation_inputs: [RelationInput.t()]
        }
end
