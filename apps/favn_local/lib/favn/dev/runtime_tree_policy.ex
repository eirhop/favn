defmodule Favn.Dev.RuntimeTreePolicy do
  @moduledoc false

  @entries ["mix.exs", "mix.lock", "config", "apps", "web/favn_web"]
  @optional_entries ["mix.exs", "mix.lock", "config"]
  @ignored_entries [
    ".elixir_ls",
    ".favn",
    ".git",
    ".svelte-kit",
    "_build",
    "cover",
    "deps",
    "dist",
    "node_modules",
    "test-results"
  ]

  @spec entries() :: [Path.t()]
  def entries, do: @entries

  @spec optional_entries() :: [Path.t()]
  def optional_entries, do: @optional_entries

  @spec ignored_entries() :: [String.t()]
  def ignored_entries, do: @ignored_entries
end
