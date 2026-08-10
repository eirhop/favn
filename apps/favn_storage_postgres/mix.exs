defmodule FavnStoragePostgres.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_storage_postgres,
      version: "0.5.0-rc.3",
      description: "Postgres storage adapter for orchestrator runtime state",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ecto_sql]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      internal_dep(:favn_orchestrator, "../favn_orchestrator", runtime: false),
      internal_dep(:favn_core, "../favn_core", runtime: false),
      internal_dep(:favn_azure, "../favn_azure", only: :test, runtime: false),
      internal_dep(:favn_runner, "../favn_runner", only: :test, runtime: false),
      internal_dep(:favn_test_support, "../favn_test_support", only: :test, runtime: false),
      {:ecto_sql, "~> 3.14"},
      {:jason, "~> 1.4"},
      {:postgrex, "~> 0.22"}
    ]
  end

  defp internal_dep(app, relative_path, opts) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        [path: relative_path]
      end

    {app, Keyword.merge(source, opts)}
  end
end
