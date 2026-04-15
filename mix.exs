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
        "cmd --app favn_test_support mix test",
        "cmd --app favn_core mix test",
        "cmd --app favn mix test",
        "cmd --app favn_runner mix test",
        "cmd --app favn_orchestrator mix test",
        "cmd --app favn_view mix test",
        "cmd --app favn_storage_postgres mix test",
        "cmd --app favn_storage_sqlite mix test",
        "cmd --app favn_duckdb mix test"
      ]
    ]
  end
end
