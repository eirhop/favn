defmodule Favn.SQLAsset.Definition do
  @moduledoc """
  Internal compiled SQL asset definition used by the SQL asset frontend.
  """

  alias Favn.Asset
  alias Favn.Asset.RelationInput
  alias Favn.SQL
  alias Favn.SQLAsset.Materialization

  @enforce_keys [:module, :asset, :sql, :template, :materialization]
  defstruct [
    :module,
    :asset,
    :sql,
    :template,
    :materialization,
    relation_inputs: [],
    sql_definitions: [],
    raw_asset: nil
  ]

  @type t :: %__MODULE__{
          module: module(),
          asset: Asset.t(),
          sql: String.t(),
          template: SQL.Template.t(),
          materialization: Materialization.t(),
          relation_inputs: [RelationInput.t()],
          sql_definitions: [SQL.Definition.t()],
          raw_asset: map() | nil
        }
end
