defmodule FavnView.Storybook.Components.AssetCataloguePage do
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.LineagePage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetCataloguePage.asset_catalogue_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 980px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :catalogue,
        attributes: %{
          assets: AssetCataloguePage.sample_assets(),
          filters: %{search: "", connection: "all", catalogue: "all"},
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: AssetCataloguePage.nav_items(),
          connection_options: AssetCataloguePage.connection_options(),
          catalogue_options: AssetCataloguePage.catalogue_options()
        }
      },
      %Variation{
        id: :lineage,
        attributes: %{
          assets: AssetCataloguePage.sample_assets(),
          filters: %{search: "", connection: "all", catalogue: "all"},
          active_mode: :lineage,
          loading: false,
          error: nil,
          nav_items: AssetCataloguePage.nav_items(:assets),
          connection_options: AssetCataloguePage.connection_options(),
          catalogue_options: AssetCataloguePage.catalogue_options(),
          lineage_graph: LineagePage.sample_graph(),
          lineage_inspector: LineagePage.sample_group_inspector(),
          lineage_loading: false,
          lineage_error: nil,
          lineage_search: "",
          lineage_zoom: 62,
          lineage_inspector_open?: true,
          lineage_canvas_hook?: false
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          assets: [],
          filters: %{search: "orders", connection: "duckdb", catalogue: "marketing"},
          active_mode: :filters,
          loading: false,
          error: nil,
          nav_items: AssetCataloguePage.nav_items(),
          connection_options: AssetCataloguePage.connection_options(),
          catalogue_options: AssetCataloguePage.catalogue_options()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          assets: [],
          filters: %{search: "", connection: "all", catalogue: "all"},
          active_mode: :list,
          loading: true,
          error: nil,
          nav_items: AssetCataloguePage.nav_items(),
          connection_options: AssetCataloguePage.connection_options(),
          catalogue_options: AssetCataloguePage.catalogue_options()
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          assets: [],
          filters: %{search: "", connection: "all", catalogue: "all"},
          active_mode: :list,
          loading: false,
          error: "load_failed",
          nav_items: AssetCataloguePage.nav_items(),
          connection_options: AssetCataloguePage.connection_options(),
          catalogue_options: AssetCataloguePage.catalogue_options()
        }
      }
    ]
  end
end
