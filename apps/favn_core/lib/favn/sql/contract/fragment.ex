defmodule Favn.SQL.Contract.Fragment do
  @moduledoc """
  Compiled column-only SQL contract fragment.

  A fragment is consumed while compiling a SQL asset. Its normalized columns
  are flattened into the asset contract; runtime manifests do not need to load
  the fragment authoring module.
  """

  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Column
  alias Favn.DSL.Compiler, as: DSLCompiler

  @enforce_keys [:module, :columns]
  defstruct [:module, columns: []]

  @type t :: %__MODULE__{module: module(), columns: [Column.t()]}

  @doc "Builds and validates a compiled contract fragment."
  @spec new!(module(), [Column.t() | map()]) :: t()
  def new!(module, columns) when is_list(columns) do
    %__MODULE__{
      module: module,
      columns: Enum.map(columns, &normalize_column/1)
    }
    |> validate!()
  end

  @doc "Validates a compiled contract fragment."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = fragment) do
    unless is_atom(fragment.module) and DSLCompiler.module_atom?(fragment.module),
      do: raise(ArgumentError, "contract fragment module must be an Elixir module atom")

    unless is_list(fragment.columns) and fragment.columns != [],
      do: raise(ArgumentError, "SQL contract fragment requires at least one column")

    if length(fragment.columns) > Contract.max_columns(),
      do:
        raise(
          ArgumentError,
          "SQL contract fragment supports at most #{Contract.max_columns()} columns"
        )

    Enum.each(fragment.columns, &Column.validate!/1)

    names = Enum.map(fragment.columns, & &1.name)

    case names -- Enum.uniq(names) do
      [] -> fragment
      [name | _rest] -> raise ArgumentError, "duplicate contract fragment column #{inspect(name)}"
    end
  end

  defp normalize_column(%Column{} = column), do: Column.validate!(column)

  defp normalize_column(%{name: name, type: type} = column) do
    opts = Map.get(column, :opts, [])
    Column.new!(name, type, opts)
  end

  defp normalize_column(other),
    do: raise(ArgumentError, "invalid contract fragment column #{inspect(other)}")
end
