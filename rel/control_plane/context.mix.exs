Code.require_file("rel/control_plane/release.exs", __DIR__)

defmodule FavnControlPlane.BuildProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      apps: FavnControlPlane.Release.applications(),
      version: "0.5.0-dev",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      releases: FavnControlPlane.Release.config()
    ]
  end
end
