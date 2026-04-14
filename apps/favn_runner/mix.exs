defmodule FavnRunner.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_runner,
      version: "0.5.0-dev",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {FavnRunner.Application, []}
    ]
  end

  defp deps do
    [
      {:favn_core, in_umbrella: true}
    ]
  end
end
