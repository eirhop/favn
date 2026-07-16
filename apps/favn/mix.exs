defmodule Favn.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn,
      version: "0.5.0-dev",
      description: "Public Favn package wrapper",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      elixirc_options: [docs: true],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      internal_dep(:favn_authoring, "../favn_authoring"),
      internal_dep(:favn_local, "../favn_local"),
      internal_dep(:favn_sql_runtime, "../favn_sql_runtime"),
      internal_dep(:favn_orchestrator, "../favn_orchestrator", only: :test),
      internal_dep(:favn_runner, "../favn_runner", only: :test),
      internal_dep(:favn_test_support, "../favn_test_support", only: :test),
      {:ex_doc, "~> 0.38", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/eirhop/favn",
      source_ref: "main",
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/authoring-assets.md",
        "guides/duckdb-session-scripts.md",
        "guides/runner-plugins.md",
        "guides/sql-runtime-inputs.md",
        "guides/retries-and-replay.md",
        "guides/sql-output-contracts.md",
        "guides/sql-asset-checks.md",
        "guides/local-development.md",
        "guides/configuration.md",
        "guides/sql-client.md",
        "guides/ai-agents.md",
        "guides/manifest-first.md",
        "guides/runtime-model.md",
        "guides/adapters.md",
        "guides/cheatsheet.cheatmd"
      ],
      groups_for_modules: [
        "Public Facades": [Favn, Favn.AI, Favn.SQLClient],
        "Runner Extensions": [
          Favn.Runner.Plugin,
          Favn.Runner.SupervisedChildren,
          Favn.RuntimeValue,
          Favn.RuntimeValue.Provider,
          Favn.RuntimeValue.Ref,
          Favn.RuntimeValue.Error
        ],
        "Mix Tasks": ~r/^Mix\.Tasks\.Favn(?:\.|$)/
      ],
      groups_for_extras: [
        Cheatsheets: ~r/cheatsheet\.cheatmd/,
        Guides: ~r/guides\//
      ]
    ]
  end

  defp internal_dep(app, relative_path, opts \\ []) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        [path: relative_path]
      end

    {app, Keyword.merge(source, opts)}
  end
end
