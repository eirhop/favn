defmodule FavnView.Storybook.Components.RebuildsPage do
  alias FavnView.Components.RebuildPage

  use PhoenixStorybook.Story, :component

  def function, do: &RebuildPage.rebuilds_page/1
  def layout, do: :one_column
  def render_source, do: :function
  def container, do: {:iframe, style: "width: 100%; height: 900px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark"><.psb-variation/></div>
    """
  end

  def variations do
    [
      %Variation{
        id: :planning,
        attributes: %{
          operations: [operation(:planned)],
          target_id: "asset:orders",
          planning?: true,
          has_more?: false
        }
      },
      %Variation{
        id: :planned,
        attributes: %{
          operations: [operation(:planned)],
          target_id: "asset:orders",
          plan: plan(),
          planning?: false,
          has_more?: false
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          operations: [],
          target_id: "",
          planning?: false,
          has_more?: false
        }
      },
      %Variation{
        id: :blocked,
        attributes: %{
          operations: [operation(:failed)],
          target_id: "asset:orders",
          error: "Administrator access is required.",
          planning?: false,
          has_more?: false
        }
      }
    ]
  end

  defp plan do
    %{
      plan_id: "rebuild_plan_01",
      plan_hash: String.duplicate("a", 64),
      expires_at: ~U[2026-07-22 14:00:00Z],
      permissions: %{start: true}
    }
  end

  defp operation(state) do
    %{
      operation_id: "rebuild_plan_01",
      root_target_id: "asset:orders",
      state: state,
      phase: :planned,
      reason: "schema changed",
      progress: %{completed: 0, total: 12},
      updated_at: ~U[2026-07-22 12:00:00Z]
    }
  end
end
