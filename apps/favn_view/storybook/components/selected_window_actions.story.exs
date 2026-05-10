defmodule FavnView.Storybook.Components.SelectedWindowActions do
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.SelectedWindowActions

  use PhoenixStorybook.Story, :component

  def function, do: &SelectedWindowActions.selected_window_actions/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 720px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg flex min-h-[22rem] items-center p-12 text-base-content">
      <div class="w-full max-w-6xl">
        <.psb-variation/>
      </div>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :runnable,
        attributes: %{
          selected_window: AssetDetailPage.selected_sample_window()
        }
      },
      %Variation{
        id: :run_window_config_default_auto,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "auto"})
      },
      %Variation{
        id: :run_window_config_missing_only,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "missing"})
      },
      %Variation{
        id: :run_window_config_force_selected_asset,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "force_selected"})
      },
      %Variation{
        id: :run_window_config_force_selected_upstream,
        attributes:
          run_config_attributes(%{dependencies: "all", refresh: "force_selected_upstream"})
      },
      %Variation{
        id: :run_window_config_force_full_graph,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "force_all"})
      },
      %Variation{
        id: :submitting,
        attributes: %{
          selected_window: AssetDetailPage.selected_sample_window(),
          submitting_window_run?: true
        }
      },
      %Variation{
        id: :success,
        attributes: %{
          selected_window: AssetDetailPage.selected_sample_window(),
          submitted_run_id: "run_01HZ"
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          selected_window: AssetDetailPage.selected_sample_window(),
          selected_window_error: "Could not submit run."
        }
      },
      %Variation{
        id: :not_runnable,
        attributes: %{
          selected_window: AssetDetailPage.selected_non_runnable_window()
        }
      }
    ]
  end

  defp run_config_attributes(run_config) do
    %{
      selected_window: AssetDetailPage.selected_sample_window(),
      run_config_open?: true,
      run_config: run_config
    }
  end
end
