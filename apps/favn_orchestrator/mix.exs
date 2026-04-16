defmodule FavnOrchestrator.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_orchestrator,
      version: "0.5.0-dev",
      description: "Internal orchestrator runtime scaffold for v0.5 migration",
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
      extra_applications: [:logger],
      mod: {FavnOrchestrator.Application, []}
    ]
  end

  defp deps do
    [
      {:favn_core, in_umbrella: true},
      {:phoenix_pubsub, "~> 2.2"}
    ]
  end
end
