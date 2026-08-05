defmodule FavnView.Dev.DesignSystem.Fixtures.RunConfig do
  @moduledoc """
  Run-configuration view models for the asset run dialog.

  The dialog and the asset detail page render the same shape, so it is built once
  here. It is a view model, not domain data: `favn_view` never reaches past the
  orchestrator facade, and these fixtures reach nowhere at all.

  Compiled in `:dev` and `:test`, so a LiveView test and the design system can
  assert against the same configuration.
  """

  alias FavnView.AssetRunConfig

  @doc """
  A run config for one period.

  Built by overriding `FavnView.AssetRunConfig.default/0` so a field added to that
  shape reaches these fixtures too, rather than leaving them silently short.
  """
  @spec run_config(atom(), atom(), String.t(), String.t(), String.t()) :: map()
  def run_config(source, kind, value, dependencies \\ "all", refresh \\ "auto") do
    Map.merge(AssetRunConfig.default(), %{
      dependencies: dependencies,
      refresh: refresh,
      source: Atom.to_string(source),
      kind: Atom.to_string(kind),
      value: value
    })
  end

  @doc """
  The run config for an asset with no window policy, which has no period to run.

  That is exactly what a dialog opens on when the backend offers no period, so it is
  `FavnView.AssetRunConfig`'s own default rather than a second copy of those fields.
  """
  @spec full_refresh_run_config() :: map()
  def full_refresh_run_config, do: AssetRunConfig.default()

  @doc """
  The period a windowed asset is due for.
  """
  @spec default_run_config() :: map()
  def default_run_config, do: run_config(:refresh_timeline, :day, "2026-06-12")
end
