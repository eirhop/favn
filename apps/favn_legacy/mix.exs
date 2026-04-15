defmodule FavnLegacy.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_legacy,
      version: "0.4.0",
      description: "Legacy monolith reference app during v0.5 refactor",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.19",
      source_url: "https://github.com/eirhop/favn",
      homepage_url: "https://github.com/eirhop/favn",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      test_ignore_filters: [~r"^test/support/"],
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      preferred_envs: [coverage: :test],
      coverage: [tool: ExCoveralls, threshold: 80]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :phoenix_pubsub, :ecto_sql],
      mod: {Favn.Application, []}
    ]
  end

  defp deps do
    [
      {:favn, in_umbrella: true},
      {:telemetry, "~> 1.3.0"},
      {:ecto, "~> 3.13.5"},
      {:ecto_sql, "~> 3.13.4"},
      {:ecto_sqlite3, "~> 0.22.0"},
      {:postgrex, "~> 0.22"},
      {:duckdbex, "~> 0.3.21"},
      {:phoenix_pubsub, "~> 2.2.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/eirhop/favn"}
    ]
  end
end
