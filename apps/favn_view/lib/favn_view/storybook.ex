defmodule FavnView.Storybook do
  @moduledoc false

  use PhoenixStorybook,
    otp_app: :favn_view,
    content_path: Path.expand("../../storybook", __DIR__),
    css_path: "/assets/css/app.css",
    title: "Favn View Storybook"
end
