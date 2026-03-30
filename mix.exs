defmodule Favn.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn,
      version: "0.1.1",
      description: "Asset-oriented workflow orchestration for Elixir applications",
      elixir: "~> 1.17",
      source_url: "https://github.com/eirhop/favn",
      homepage_url: "https://github.com/eirhop/favn",
      docs: [
        main: "readme",
        extras: ["README.md"]
      ],
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :phoenix_pubsub, :ecto_sql],
      mod: {Favn.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:decimal, git: "https://github.com/ericmj/decimal.git", tag: "v2.3.0", override: true},
      {:telemetry,
       git: "https://github.com/beam-telemetry/telemetry.git", tag: "v1.3.0", override: true},
      {:db_connection,
       git: "https://github.com/elixir-ecto/db_connection.git", tag: "v2.8.1", override: true},
      {:elixir_make,
       git: "https://github.com/elixir-lang/elixir_make.git", tag: "v0.9.0", override: true},
      {:cc_precompiler,
       git: "https://github.com/cocoa-xu/cc_precompiler.git", tag: "v0.1.11", override: true},
      {:exqlite,
       git: "https://github.com/elixir-sqlite/exqlite.git", tag: "v0.22.0", override: true},
      {:ecto, git: "https://github.com/elixir-ecto/ecto.git", tag: "v3.13.5", override: true},
      {:ecto_sql,
       git: "https://github.com/elixir-ecto/ecto_sql.git", tag: "v3.13.4", override: true},
      {:ecto_sqlite3, git: "https://github.com/elixir-sqlite/ecto_sqlite3.git", tag: "v0.22.0"},
      {:phoenix_pubsub,
       git: "https://github.com/phoenixframework/phoenix_pubsub.git", tag: "v2.2.0"}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/eirhop/favn"}
    ]
  end
end
