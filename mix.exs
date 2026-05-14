defmodule FavnUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      apps: [
        :favn,
        :favn_authoring,
        :favn_azure,
        :favn_core,
        :favn_duckdb,
        :favn_duckdb_adbc,
        :favn_local,
        :favn_orchestrator,
        :favn_runner,
        :favn_sql_runtime,
        :favn_storage_postgres,
        :favn_storage_sqlite,
        :favn_test_support,
        :favn_view
      ],
      version: "0.5.0-dev",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  def cli do
    [
      preferred_envs: [
        "test.fast": :test,
        "test.acceptance": :test
      ]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      test: Enum.map(test_apps(), &"do --app #{&1} test"),
      "test.fast": fast_test_alias(),
      "test.acceptance": ["do --app favn_local cmd mix test --only acceptance --slowest 20"]
    ]
  end

  defp fast_test_alias do
    Enum.map(test_apps(), fn app ->
      "do --app #{app} cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser"
    end)
  end

  defp test_apps do
    [
      :favn_test_support,
      :favn_core,
      :favn_authoring,
      :favn_azure,
      :favn,
      :favn_sql_runtime,
      :favn_runner,
      :favn_orchestrator,
      :favn_storage_postgres,
      :favn_storage_sqlite,
      :favn_duckdb,
      :favn_duckdb_adbc,
      :favn_local,
      :favn_view
    ]
  end
end
