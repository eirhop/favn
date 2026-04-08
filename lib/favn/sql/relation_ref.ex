defmodule Favn.SQL.RelationRef do
  @moduledoc """
  Canonical requested relation identity used in adapter introspection calls.
  """

  @enforce_keys [:name]
  defstruct [:catalog, :schema, :name]

  @type t :: %__MODULE__{
          catalog: binary() | nil,
          schema: binary() | nil,
          name: binary()
        }
end
