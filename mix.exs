defmodule FavnUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      apps: [
        :favn,
        :favn_authoring,
        :favn_core,
        :favn_duckdb,
        :favn_local,
        :favn_orchestrator,
        :favn_runner,
        :favn_storage_postgres,
        :favn_storage_sqlite,
        :favn_test_support
      ],
      version: "0.5.0-dev",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      test: [
        "do --app favn_test_support test",
        "do --app favn_core test",
        "do --app favn_authoring test",
        "do --app favn test",
        "do --app favn_runner test",
        "do --app favn_orchestrator test",
        "do --app favn_storage_postgres test",
        "do --app favn_storage_sqlite test",
        "do --app favn_duckdb test",
        "do --app favn_local test"
      ]
    ]
  end
end
