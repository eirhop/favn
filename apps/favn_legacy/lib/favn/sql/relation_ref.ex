defmodule Favn.SQL.RelationRef do
  @moduledoc """
  Backward-compatible alias for the shared canonical relation identity.
  """

  @enforce_keys [:name]
  defstruct [:connection, :catalog, :schema, :name]

  @type t :: Favn.RelationRef.t()
end
