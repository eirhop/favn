defmodule FavnCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_core,
      version: "0.5.0-dev",
      description: "Internal core contracts scaffold for v0.5 migration",
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
    [
      {:jason, "~> 1.4"},
      internal_dep(:favn_test_support, "../favn_test_support", only: :test)
    ]
  end

  defp internal_dep(app, sparse_path, opts) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        [path: sparse_path]
      end

    {app, Keyword.merge(source, opts)}
  end
end
