defmodule CrmDemo.MixProject do
  use Mix.Project

  def project do
    [
      app: :crm_demo,
      version: "0.1.0-dev",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:favn, path: "../../apps/favn"},
      {:favn_duckdb_adbc, path: "../../apps/favn_duckdb_adbc"}
    ]
  end
end
