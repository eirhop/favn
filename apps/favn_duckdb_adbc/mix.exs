defmodule FavnDuckdbADBC.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_duckdb_adbc,
      version: "0.5.0-dev",
      description: "DuckDB ADBC SQL adapter and runner plugin for Favn",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      test_ignore_filters: [~r/test\/support\//],
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      internal_dep(:favn_runner, "../favn_runner"),
      internal_dep(:favn_sql_runtime, "../favn_sql_runtime"),
      internal_dep(:favn_authoring, "../favn_authoring", only: :test),
      internal_dep(:favn_test_support, "../favn_test_support", only: :test),
      {:adbc, "~> 0.12"}
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
