defmodule FavnView.Dev.DesignSystem.Examples do
  @moduledoc """
  The curated examples, keyed by catalogue entry id.

  Each provider module returns a plain `%{entry_id => [%Example{}]}` map. There
  is no registration macro and no per-component file: a provider is data plus a
  handful of HEEx render functions, and an entry with no key here is reported as
  a gap rather than silently absent.

  Providers are split by layer, matching `FavnView.UI`:

  | Provider | Covers |
  | --- | --- |
  | `Examples.Elements` | `FavnView.UI.*` |
  | `Examples.Components` | `FavnView.Components.*` sections |
  | `Examples.Pages` | whole screens, built from shared fixtures |
  """

  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Examples

  @providers [Examples.Elements, Examples.Components, Examples.Pages]

  @doc """
  Curated examples for one entry id.
  """
  @spec for_entry(String.t()) :: [Example.t()]
  def for_entry(id) do
    Enum.flat_map(@providers, fn provider -> Map.get(examples(provider), id, []) end)
  end

  @doc """
  Every entry id that has at least one curated example.
  """
  @spec covered() :: MapSet.t(String.t())
  def covered do
    @providers
    |> Enum.flat_map(&Map.keys(examples(&1)))
    |> MapSet.new()
  end

  # A provider's map builds every fixture it uses, and the index page asks for
  # every entry, so an uncached walk builds each provider's fixtures once per
  # card — 186 rebuilds in one request. The memo is per-process because that is
  # the whole requirement: a request dedupes its own walk and then dies. A
  # longer-lived cache went stale the first time a *dependency* of a provider
  # (a view model the fixtures call) was recompiled, because no key derivable
  # from the provider alone can see that edit.
  defp examples(provider) do
    key = {__MODULE__, provider}

    case Process.get(key, :missing) do
      :missing ->
        map = provider.all()
        Process.put(key, map)
        map

      map ->
        map
    end
  end
end
