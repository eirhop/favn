defmodule FavnReferenceWorkload.MixProject do
  use Mix.Project

  def project do
    [
      app: :basic_workflow_tutorial,
      version: "0.1.0-dev",
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
      {:favn, path: "../../apps/favn"},
      {:favn_duckdb, path: "../../apps/favn_duckdb"}
    ]
  end
end
