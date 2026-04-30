defmodule Favn.SQLAsset.Definition do
  @moduledoc """
  Internal compiled SQL asset definition used by the SQL asset frontend.
  """

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

  @type asset :: %{
          required(:ref) => {module(), atom()},
          required(:relation) => Favn.RelationRef.t(),
          required(:file) => String.t(),
          required(:window_spec) => Favn.Window.Spec.t() | nil,
          optional(atom()) => term()
        }

  @type t :: %__MODULE__{
          module: module(),
          asset: asset(),
          sql: String.t(),
          template: SQL.Template.t(),
          materialization: Materialization.t(),
          relation_inputs: [RelationInput.t()],
          sql_definitions: [SQL.Definition.t()],
          raw_asset: map() | nil
        }
end
