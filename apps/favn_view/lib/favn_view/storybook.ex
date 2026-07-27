defmodule FavnView.Storybook do
  @moduledoc false

  @css_asset_path Path.expand("../../priv/static/assets/css/app.css", __DIR__)
  @external_resource @css_asset_path

  use PhoenixStorybook,
    otp_app: :favn_view,
    content_path: Path.expand("../../storybook", __DIR__),
    css_path:
      if(File.regular?(Path.expand("../../priv/static/assets/css/app.css", __DIR__)),
        do: "/assets/css/app.css"
      ),
    title: "Favn View Storybook"
end
