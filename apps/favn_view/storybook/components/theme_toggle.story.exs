defmodule FavnView.Storybook.Components.ThemeToggle do
  alias FavnView.Components.ThemeToggle

  use PhoenixStorybook.Story, :component

  def function, do: &ThemeToggle.theme_toggle/1
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{id: :default}
    ]
  end
end
