defmodule FavnTestSupport do
  @moduledoc """
  Shared test support infrastructure for umbrella apps.

  This app owns cross-app fixture source files and small helper APIs that keep
  test fixture loading deterministic and dependency-light.
  """

  @doc """
  Adds the canonical dependency graph to a manifest fixture.

  The graph builder is invoked dynamically so this dependency-light support
  app can serve `favn_core` tests without creating an umbrella dependency
  cycle.
  """
  @spec with_manifest_graph(map()) :: map()
  def with_manifest_graph(%{assets: assets} = manifest) when is_list(assets) do
    {:ok, graph} = apply(Favn.Manifest.Graph, :build, [assets])
    Map.put(manifest, :graph, graph)
  end
end
