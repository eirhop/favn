defmodule Favn.SQL.Relation do
  @moduledoc """
  Normalized discovered relation metadata.
  """

  @enforce_keys [:name, :type]
  defstruct [:catalog, :schema, :name, :type, metadata: %{}]

  @type relation_type :: :table | :view | :materialized_view | :temporary | :unknown

  @type t :: %__MODULE__{
          catalog: binary() | nil,
          schema: binary() | nil,
          name: binary(),
          type: relation_type(),
          metadata: map()
        }
end
