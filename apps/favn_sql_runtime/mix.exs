defmodule FavnSQLRuntime.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_sql_runtime,
      version: "0.5.0-dev",
      description: "Shared SQL runtime contracts and client",
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
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:favn_core, in_umbrella: true},
      {:favn_test_support, in_umbrella: true, only: :test}
    ]
  end
end
