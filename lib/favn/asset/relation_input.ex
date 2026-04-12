defmodule Favn.Asset.RelationInput do
  @moduledoc """
  Typed SQL relation input discovered from normalized SQL IR.
  """

  alias Favn.RelationRef
  alias Favn.SQL.Template.Span

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
          span: Span.t() | nil
        }
end
