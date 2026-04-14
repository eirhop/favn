defmodule Favn.PublicScaffold do
  @moduledoc """
  Public `favn` app scaffold for v0.5 umbrella migration.

  During Phase 1, legacy runtime modules remain in `favn_legacy`.
  This module intentionally avoids defining public API functions until
  Phase 2 migrates DSL and facade responsibilities from legacy.
  """

  @doc """
  Returns the scaffold status for the public app.

  ## Examples

      iex> Favn.PublicScaffold.status()
      :phase_1_scaffolded
  """
  def status, do: :phase_1_scaffolded
end
