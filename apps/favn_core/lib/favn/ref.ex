defmodule Favn.Ref do
  @moduledoc """
  Canonical asset references.

  Favn uses `{module, name}` tuples as the public and internal reference shape
  for assets.
  """

  @typedoc """
  Canonical reference to an asset.
  """
  @type t :: {module(), atom()}

  @doc """
  Build a canonical asset reference.
  """
  @spec new(module(), atom()) :: t()
  def new(module, name) when is_atom(module) and is_atom(name), do: {module, name}
end
