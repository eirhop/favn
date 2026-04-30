defmodule FavnCore do
  @moduledoc """
  Internal application marker for the `:favn_core` app.

  The shared contracts and implementation modules in this app live under the
  `Favn.*` namespace. This module is kept as a lightweight app-level smoke
  surface for the generated Mix project scaffold.
  """

  @doc """
  Returns the existing scaffold smoke value.

  ## Examples

      iex> FavnCore.hello()
      :world

  """
  def hello do
    :world
  end
end
