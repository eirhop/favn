defmodule FavnStoragePostgres.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_storage_postgres,
      version: "0.5.0-dev",
      description: "Postgres storage adapter for orchestrator runtime state",
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
      extra_applications: [:logger, :ecto_sql]
    ]
  end

  defp deps do
    [
      internal_dep(:favn_orchestrator, "../favn_orchestrator"),
      internal_dep(:favn_test_support, "../favn_test_support", only: :test),
      {:ecto_sql, "~> 3.13.4"},
      {:postgrex, "~> 0.22"}
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
