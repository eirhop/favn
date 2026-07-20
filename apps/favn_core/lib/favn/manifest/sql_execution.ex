defmodule Favn.Manifest.SQLExecution do
  @moduledoc """
  Canonical SQL execution payload carried by immutable execution packages.

  The optional typed `contract` preserves authored output shape, grain, keys,
  ordered row-count policies, and lineage. Generated contract checks and custom checks are
  both carried in `checks`, distinguished by their origin and claim identity.
  Compact manifest assets reference the package by content hash, so the runner
  needs no authoring module to validate a candidate and catalogue operations do
  not load executable SQL.
  """

  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Check
  alias Favn.SQL.Contract
  alias Favn.SQL.Template
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.SQLAsset.Definition

  @enforce_keys [:sql, :template]
  defstruct [:sql, :template, :runtime_inputs, :contract, sql_definitions: [], checks: []]

  @type t :: %__MODULE__{
          sql: String.t(),
          template: Template.t(),
          runtime_inputs: RuntimeInputResolverRef.t() | nil,
          contract: Contract.t() | nil,
          sql_definitions: [SQLDefinition.t()],
          checks: [Check.t()]
        }

  @spec from_definition(Definition.t()) :: t()
  def from_definition(%Definition{} = definition) do
    %__MODULE__{
      sql: definition.sql,
      template: definition.template,
      runtime_inputs: definition.runtime_inputs,
      contract: definition.contract,
      sql_definitions: definition.sql_definitions,
      checks: definition.checks
    }
  end
end
