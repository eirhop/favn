defmodule FavnView.Storybook.Components.RebuildDetailPage do
  alias FavnView.Components.RebuildPage

  use PhoenixStorybook.Story, :component

  def function, do: &RebuildPage.rebuild_detail_page/1
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
        id: :running,
        attributes: %{
          operation: operation(:building),
          items: items(),
          items_has_more?: true
        }
      },
      %Variation{
        id: :activation_unknown,
        attributes: %{
          operation:
            Map.merge(operation(:activation_unknown), %{
              phase: :activation,
              unknown_outcome: %{kind: "activation_commit_unknown"},
              permissions: %{start: false, cancel: false, retry: false, reconcile: true}
            }),
          items: items(),
          items_has_more?: false
        }
      },
      %Variation{
        id: :succeeded,
        attributes: %{
          operation:
            Map.merge(operation(:succeeded), %{
              phase: :terminal,
              progress: %{completed: 12, total: 12},
              cleanup_state: :complete,
              permissions: %{start: false, cancel: false, retry: false, reconcile: false}
            }),
          items: items(),
          items_has_more?: false
        }
      },
      %Variation{
        id: :failed,
        attributes: %{
          operation:
            Map.merge(operation(:failed), %{
              phase: :terminal,
              terminal_error: %{
                code: "candidate_validation_failed",
                message: "Candidate validation failed."
              },
              permissions: %{start: false, cancel: true, retry: true, reconcile: false}
            }),
          items: items(),
          items_has_more?: false
        }
      }
    ]
  end

  defp operation(state) do
    %{
      operation_id: "rebuild_01",
      root_target_id: "asset:orders",
      state: state,
      phase: :build,
      progress: %{completed: 7, total: 12},
      active_generation_id: "generation_01",
      candidate_generation_id: "generation_02",
      plan_hash: String.duplicate("b", 64),
      cleanup_state: :not_started,
      permissions: %{start: false, cancel: true, retry: false, reconcile: false},
      terminal_error: nil
    }
  end

  defp items do
    [
      %{
        target_id: "asset:orders",
        item_id: "item_01",
        window_key: "month:2026-06",
        status: :succeeded,
        attempt_count: 1,
        row_count: 42_018
      },
      %{
        target_id: "asset:orders",
        item_id: "item_02",
        window_key: "month:2026-07",
        status: :running,
        attempt_count: 1,
        row_count: nil
      }
    ]
  end
end
