defmodule FavnDuckdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_duckdb,
      version: "0.5.0-dev",
      description: "DuckDB runner plugin scaffold for v0.5 migration",
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
      internal_dep(:favn_runner, "../favn_runner"),
      internal_dep(:favn_sql_runtime, "../favn_sql_runtime"),
      {:duckdbex, "~> 0.3.21"}
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
