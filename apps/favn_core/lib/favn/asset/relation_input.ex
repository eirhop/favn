defmodule Favn.Asset.RelationInput do
  @moduledoc """
  Typed SQL relation input bound from normalized SQL IR.

  Plain SQL identifiers become runtime relation inputs only when they match a
  declared dependency relation. Unbound identifier candidates remain ordinary
  SQL text.
  """

  alias Favn.RelationRef

  @enforce_keys [:kind]
  defstruct [
    :kind,
    :relation_ref,
    :raw,
    :asset_ref,
    :resolution,
    :span
  ]

  @type kind :: :plain_relation | :direct_asset_ref

  @type resolution :: :resolved | :deferred | nil

  @type t :: %__MODULE__{
          kind: kind(),
          relation_ref: RelationRef.t() | nil,
          raw: String.t() | nil,
          asset_ref: Favn.Ref.t() | nil,
          resolution: resolution(),
          span: map() | nil
        }
end
