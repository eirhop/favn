defmodule FavnView.Storybook.Components.LineagePage do
  alias FavnView.Components.LineagePage

  use PhoenixStorybook.Story, :component

  def function, do: &LineagePage.lineage_page/1
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
    graph = LineagePage.sample_graph()

    [
      %Variation{
        id: :full_page,
        attributes: %{
          graph: graph,
          inspector: LineagePage.sample_group_inspector(),
          view_mode: :all,
          search: "",
          loading: false,
          error: nil,
          zoom: 62,
          canvas_hook?: false
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          graph: nil,
          inspector: nil,
          view_mode: :all,
          search: "",
          loading: false,
          error: nil,
          zoom: 62,
          canvas_hook?: false
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          graph: nil,
          inspector: nil,
          view_mode: :all,
          search: "",
          loading: true,
          error: nil,
          zoom: 62,
          canvas_hook?: false
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          graph: nil,
          inspector: nil,
          view_mode: :all,
          search: "",
          loading: false,
          error: %{message: "No active manifest is available."},
          zoom: 62,
          canvas_hook?: false
        }
      }
    ]
  end
end
