defmodule Favn.SQL.Contract.Composition do
  @moduledoc """
  Manifest-visible origin of one contiguous contract-fragment inclusion.

  `start_index` is zero-based and `columns` contains only the ordered names in
  the corresponding flattened contract slice.
  """

  @enforce_keys [:module, :start_index, :columns]
  defstruct [:module, :start_index, columns: []]

  alias Favn.DSL.Compiler, as: DSLCompiler

  @type t :: %__MODULE__{
          module: module(),
          start_index: non_neg_integer(),
          columns: [atom()]
        }

  @doc "Builds and validates fragment-composition provenance."
  @spec new!(module(), non_neg_integer(), [atom()]) :: t()
  def new!(module, start_index, columns) do
    %__MODULE__{module: module, start_index: start_index, columns: columns}
    |> validate!()
  end

  @doc "Validates fragment-composition provenance independently of its contract."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = composition) do
    unless is_atom(composition.module) and DSLCompiler.module_atom?(composition.module),
      do: raise(ArgumentError, "contract composition module must be an Elixir module atom")

    unless is_integer(composition.start_index) and composition.start_index >= 0,
      do: raise(ArgumentError, "contract composition start_index must be non-negative")

    unless is_list(composition.columns) and composition.columns != [] and
             Enum.all?(composition.columns, &(is_atom(&1) and not is_nil(&1))),
           do: raise(ArgumentError, "contract composition requires ordered column atoms")

    if composition.columns != Enum.uniq(composition.columns),
      do: raise(ArgumentError, "contract composition contains duplicate columns")

    composition
  end
end
