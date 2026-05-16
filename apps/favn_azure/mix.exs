defmodule FavnAzure.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_azure,
      version: "0.5.0-dev",
      description: "Azure integration helpers for Favn adapters",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :inets, :ssl]]
  end

  defp deps do
    [
      {:jason, "~> 1.4"}
    ]
  end
end
