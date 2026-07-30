defmodule FavnView.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_view,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      compilers: [:phoenix_live_view] ++ Mix.compilers(),
      listeners: listeners(Mix.env())
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {FavnView.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  @doc """
  The paths compiled in each environment.

  `dev/` holds the design-system browser and its fixtures. It is compiled in
  `:dev`, where the browser is served, and in `:test`, where the fixtures are
  shared with component tests — but never in `:prod`, so a release contains none
  of it and no configuration mistake can expose it.

  This is public so `test/favn_view/design_system_isolation_test.exs` can assert
  the `:prod` value directly instead of inferring it.
  """
  def elixirc_paths(:dev), do: ["lib", "dev"]
  def elixirc_paths(:test), do: ["lib", "dev", "test/support"]
  def elixirc_paths(_env), do: ["lib"]

  defp listeners(:dev), do: [Phoenix.CodeReloader]
  defp listeners(_env), do: []

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.8.7"},
      {:phoenix_html, "~> 4.1"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:phoenix_live_view, "~> 1.2"},
      {:tidewave, "~> 0.5", only: :dev},
      {:lazy_html, ">= 0.1.0", only: :test},
      {:phoenix_live_dashboard, "~> 0.8.3"},
      {:esbuild, "~> 0.10", runtime: Mix.env() == :dev},
      {:tailwind, "~> 0.3", runtime: Mix.env() == :dev},
      {:heroicons,
       github: "tailwindlabs/heroicons",
       tag: "v2.2.0",
       sparse: "optimized",
       app: false,
       compile: false,
       depth: 1},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_poller, "~> 1.0"},
      {:gettext, "~> 1.0"},
      {:jason, "~> 1.2"},
      internal_dep(:favn_orchestrator, "../favn_orchestrator"),
      {:bandit, "~> 1.5"}
    ]
  end

  defp internal_dep(app, relative_path) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        [path: relative_path]
      end

    {app, source}
  end

  # Aliases are shortcuts or tasks specific to the current project.
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      setup: ["deps.get", "assets.setup", "assets.build"],
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"],
      "assets.build": ["tailwind favn_view", "esbuild favn_view", "compile"],
      "assets.deploy": [
        "tailwind favn_view --minify",
        "esbuild favn_view --minify",
        "compile",
        "phx.digest"
      ]
    ]
  end
end
