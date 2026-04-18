defmodule FavnView do
  @moduledoc """
  Archived transitional LiveView prototype.

  `favn_view` is frozen in Phase 8 and is not the steady-state product boundary.
  The active boundary is `favn_web -> favn_orchestrator` over remote HTTP/SSE.
  """

  @spec static_paths() :: [String.t()]
  def static_paths, do: ~w(assets fonts images favicon.ico robots.txt)
end
