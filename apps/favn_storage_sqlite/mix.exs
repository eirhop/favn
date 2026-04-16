defmodule FavnStorageSqlite.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_storage_sqlite,
      version: "0.5.0-dev",
      description: "SQLite storage adapter for orchestrator runtime state",
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
      {:favn_orchestrator, in_umbrella: true},
      {:ecto_sql, "~> 3.13.4"},
      {:ecto_sqlite3, "~> 0.22.0"}
    ]
  end
end
