defmodule Favn.SQL.Contract.UniqueKey do
  @moduledoc "A structured single- or multi-column uniqueness claim."

  @enforce_keys [:columns]
  defstruct [:columns]

  @type t :: %__MODULE__{columns: [atom()]}

  @doc "Builds and validates a uniqueness claim."
  @spec new!([atom()]) :: t()
  def new!(columns), do: %__MODULE__{columns: columns} |> validate!()

  @doc "Validates a compiled or rehydrated uniqueness claim."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{columns: columns} = key) do
    unless is_list(columns) and columns != [] and
             Enum.all?(columns, &(is_atom(&1) and not is_nil(&1))),
           do: raise(ArgumentError, "contract unique key must contain at least one column atom")

    if columns != Enum.uniq(columns),
      do: raise(ArgumentError, "contract unique key contains duplicate columns")

    key
  end
end
