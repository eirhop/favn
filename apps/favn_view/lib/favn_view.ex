defmodule FavnView do
  @moduledoc """
  View runtime for operator-facing LiveView screens backed by the orchestrator.
  """

  @spec static_paths() :: [String.t()]
  def static_paths, do: ~w(assets fonts images favicon.ico robots.txt)
end
