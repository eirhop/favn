defmodule FavnView.Dev.DesignSystem.Fixtures.RunConfig do
  @moduledoc """
  Run-configuration view models for the asset run dialog.

  The dialog and the asset detail page render the same shape, so it is built once
  here. It is a view model, not domain data: `favn_view` never reaches past the
  orchestrator facade, and these fixtures reach nowhere at all.

  Compiled in `:dev` and `:test`, so a LiveView test and the design system can
  assert against the same configuration.
  """

  @doc """
  A run config for one period.
  """
  @spec run_config(atom(), atom(), String.t(), String.t(), String.t()) :: map()
  def run_config(source, kind, value, dependencies \\ "all", refresh \\ "auto") do
    %{
      dependencies: dependencies,
      refresh: refresh,
      source: Atom.to_string(source),
      kind: Atom.to_string(kind),
      value: value,
      to: "",
      timezone: "Etc/UTC"
    }
  end

  @doc """
  The run config for an asset with no window policy, which has no period to run.
  """
  @spec full_refresh_run_config() :: map()
  def full_refresh_run_config do
    %{
      dependencies: "all",
      refresh: "auto",
      source: nil,
      kind: "",
      value: "",
      to: "",
      timezone: "Etc/UTC"
    }
  end

  @doc """
  The period a windowed asset is due for.
  """
  @spec default_run_config() :: map()
  def default_run_config, do: run_config(:refresh_timeline, :day, "2026-06-12")
end
