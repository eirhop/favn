defmodule FavnTestSupport.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_test_support,
      version: "0.5.0-dev",
      description: "Internal shared test fixtures and loader helpers for umbrella apps",
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
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    []
  end
end
