defmodule FavnView.Storybook.Components.Icon do
  alias FavnView.CoreComponents

  use PhoenixStorybook.Story, :component

  def function, do: &CoreComponents.icon/1

  def render_source, do: :function

  def variations do
    [
      %Variation{
        id: :placeholder,
        attributes: %{
          name: "hero-information-circle",
          class: "size-6 text-base-content"
        }
      }
    ]
  end
end
