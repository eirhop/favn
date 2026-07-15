defmodule Favn.SQL.Contract.Grain do
  @moduledoc """
  Structured and descriptive row identity for a SQL output contract.

  `by` is machine-checkable. `description` can stand alone when the grain
  cannot be represented by output columns, but descriptive grain does not
  pretend to be an automated uniqueness guarantee.
  """

  @enforce_keys [:by]
  defstruct by: [], description: nil

  @type t :: %__MODULE__{by: [atom()], description: String.t() | nil}

  @doc "Builds and validates a grain declaration."
  @spec new!(keyword() | map()) :: t()
  def new!(opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)

    %__MODULE__{by: Map.get(opts, :by, []), description: Map.get(opts, :description)}
    |> validate!()
  end

  @doc "Validates a compiled or rehydrated grain declaration."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = grain) do
    unless is_list(grain.by) and Enum.all?(grain.by, &(is_atom(&1) and not is_nil(&1))),
      do: raise(ArgumentError, "contract grain by: must be a list of column atoms")

    if grain.by != Enum.uniq(grain.by),
      do: raise(ArgumentError, "contract grain by: contains duplicate columns")

    unless is_nil(grain.description) or
             (is_binary(grain.description) and String.trim(grain.description) != ""),
           do: raise(ArgumentError, "contract grain description must be a non-empty string")

    if grain.by == [] and is_nil(grain.description),
      do: raise(ArgumentError, "contract grain requires by: or description:")

    grain
  end
end
