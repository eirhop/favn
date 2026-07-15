defmodule Favn.Manifest.SQLExecution do
  @moduledoc """
  Canonical SQL execution payload carried by manifest SQL assets.
  """

  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Check
  alias Favn.SQL.Template
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQLAsset.Definition

  @enforce_keys [:sql, :template]
  defstruct [:sql, :template, :runtime_inputs, sql_definitions: [], checks: []]

  @type t :: %__MODULE__{
          sql: String.t(),
          template: Template.t(),
          runtime_inputs: RuntimeInputResolverRef.t() | nil,
          sql_definitions: [SQLDefinition.t()],
          checks: [Check.t()]
        }

  @spec from_definition(Definition.t()) :: t()
  def from_definition(%Definition{} = definition) do
    %__MODULE__{
      sql: definition.sql,
      template: definition.template,
      runtime_inputs: definition.runtime_inputs,
      sql_definitions: definition.sql_definitions,
      checks: definition.checks
    }
  end
end
