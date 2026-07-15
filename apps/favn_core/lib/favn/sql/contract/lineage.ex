defmodule Favn.SQL.Contract.Lineage do
  @moduledoc """
  Typed, explicitly authored origin for one contracted SQL output column.

  Internal origins identify an asset and column. External origins identify a
  stable dataset and field. The authoring DSL accepts plain tuples and compiles
  them into this struct so manifests never persist ambiguous tuple shapes.
  """

  @enforce_keys [:kind, :column]
  defstruct [:kind, :asset_ref, :dataset, :column]

  @type t :: %__MODULE__{
          kind: :asset | :external,
          asset_ref: Favn.Ref.t() | nil,
          dataset: String.t() | nil,
          column: atom() | String.t()
        }

  @doc "Normalizes one explicit lineage tuple."
  @spec new!(term()) :: t()
  def new!({{module, asset_name}, column})
      when is_atom(module) and is_atom(asset_name) and is_atom(column) do
    %__MODULE__{kind: :asset, asset_ref: {module, asset_name}, column: column}
  end

  def new!({module, column}) when is_atom(module) and is_atom(column) do
    %__MODULE__{kind: :asset, asset_ref: {module, :asset}, column: column}
  end

  def new!({dataset, field}) when is_binary(dataset) and is_binary(field) do
    if String.trim(dataset) == "" or String.trim(field) == "" do
      raise ArgumentError, "external lineage dataset and field must not be empty"
    end

    %__MODULE__{kind: :external, dataset: dataset, column: field}
  end

  def new!(other) do
    raise ArgumentError,
          "invalid column lineage #{inspect(other)}; expected {Module, :column}, " <>
            "{{Module, :asset_name}, :column}, or {\"external.dataset\", \"field\"}"
  end

  @doc "Validates a rehydrated lineage value."
  @spec validate!(t()) :: t()
  def validate!(
        %__MODULE__{
          kind: :asset,
          asset_ref: {module, name},
          dataset: nil,
          column: column
        } = lineage
      )
      when is_atom(module) and is_atom(name) and is_atom(column),
      do: lineage

  def validate!(
        %__MODULE__{
          kind: :external,
          asset_ref: nil,
          dataset: dataset,
          column: column
        } = lineage
      )
      when is_binary(dataset) and dataset != "" and is_binary(column) and column != "",
      do: lineage

  def validate!(lineage),
    do: raise(ArgumentError, "invalid typed column lineage #{inspect(lineage)}")
end
