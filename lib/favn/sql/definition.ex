defmodule Favn.SQL.Definition do
  @moduledoc """
  Compiled reusable SQL definition declared via `defsql`.
  """

  alias Favn.SQL.Template

  @type shape :: :expression | :relation

  @enforce_keys [:module, :name, :arity, :params, :shape, :sql, :template, :file, :line]
  defstruct [:module, :name, :arity, :params, :shape, :sql, :template, :file, :line]

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          arity: non_neg_integer(),
          params: [atom()],
          shape: shape(),
          sql: String.t(),
          template: Template.t(),
          file: String.t(),
          line: pos_integer()
        }
end
