defmodule FavnLocal.TestSupport.DrainAsset do
  @moduledoc false

  use Favn.Asset

  def asset(_context), do: :ok
end
