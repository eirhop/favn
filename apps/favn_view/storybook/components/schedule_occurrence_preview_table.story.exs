defmodule FavnView.Storybook.Components.ScheduleOccurrencePreviewTable do
  alias FavnView.Components.ScheduleUi

  use PhoenixStorybook.Story, :component

  def function, do: &ScheduleUi.occurrence_preview_table/1
  def layout, do: :one_column
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-screen p-8">
      <div class="favn-surface-panel rounded-box border border-base-content/10 bg-base-100/35 p-4">
        <.psb-variation/>
      </div>
    </div>
    """
  end

  def variations do
    [
      %Variation{id: :preview_rows, attributes: %{occurrences: ScheduleUi.sample_occurrences()}},
      %Variation{id: :empty, attributes: %{occurrences: []}}
    ]
  end
end
