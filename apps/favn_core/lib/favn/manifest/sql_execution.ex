defmodule Favn.Manifest.SQLExecution do
  @moduledoc """
  Canonical SQL execution payload carried by manifest SQL assets.
  """

  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template
  alias Favn.SQLAsset.Definition

  @enforce_keys [:sql, :template]
  defstruct [:sql, :template, sql_definitions: []]

  @type t :: %__MODULE__{
          sql: String.t(),
          template: Template.t(),
          sql_definitions: [SQLDefinition.t()]
        }

  @spec from_definition(Definition.t()) :: t()
  def from_definition(%Definition{} = definition) do
    %__MODULE__{
      sql: definition.sql,
      template: definition.template,
      sql_definitions: definition.sql_definitions
    }
  end
end
