defmodule Favn.SQL.Definition do
  @moduledoc """
  Compiled reusable SQL definition declared via `defsql`.
  """

  alias Favn.SQL.Template

  @type shape :: :expression | :relation

  defmodule Param do
    @moduledoc false
    @enforce_keys [:name, :index]
    defstruct [:name, :index]

    @type t :: %__MODULE__{name: atom(), index: non_neg_integer()}
  end

  @enforce_keys [:module, :name, :arity, :params, :shape, :sql, :template, :file, :line]
  defstruct [:module, :name, :arity, :params, :shape, :sql, :template, :file, :line]

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          arity: non_neg_integer(),
          params: [Param.t()],
          shape: shape(),
          sql: String.t(),
          template: Template.t(),
          file: String.t(),
          line: pos_integer()
        }

  @spec key(t()) :: {atom(), non_neg_integer()}
  def key(%__MODULE__{name: name, arity: arity}), do: {name, arity}
end
