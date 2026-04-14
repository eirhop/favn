defmodule FavnUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.5.0-dev",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  defp deps do
    []
  end
end
