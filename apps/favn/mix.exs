defmodule Favn.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn,
      version: "0.5.0-dev",
      description: "Public Favn package wrapper",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      elixirc_options: [docs: true],
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
      internal_dep(:favn_authoring, "../favn_authoring"),
      internal_dep(:favn_local, "../favn_local"),
      internal_dep(:favn_sql_runtime, "../favn_sql_runtime"),
      internal_dep(:favn_orchestrator, "../favn_orchestrator", only: :test),
      internal_dep(:favn_runner, "../favn_runner", only: :test),
      internal_dep(:favn_test_support, "../favn_test_support", only: :test)
    ]
  end

  defp internal_dep(app, relative_path, opts \\ []) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        [path: relative_path]
      end

    {app, Keyword.merge(source, opts)}
  end
end
