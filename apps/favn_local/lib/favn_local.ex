defmodule FavnLocal do
  @moduledoc """
  Owner app boundary for local runtime lifecycle tooling.

  `favn_local` owns project-local stack lifecycle behavior (`mix favn.dev`,
  `mix favn.stop`, `mix favn.reload`, `mix favn.status`) and `.favn/` runtime
  state management.

  The public authoring/build API remains in `apps/favn`.
  """
end
