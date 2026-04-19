defmodule FavnUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.5.0-dev",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  defp deps do
    []
  end

  defp aliases do
    [
      test: [
        "do --app favn_test_support test",
        "do --app favn_core test",
        "do --app favn test",
        "do --app favn_runner test",
        "do --app favn_orchestrator test",
        "do --app favn_storage_postgres test",
        "do --app favn_storage_sqlite test",
        "do --app favn_duckdb test"
      ]
    ]
  end
end
