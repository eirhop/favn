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
      internal_dep(:favn_authoring, "apps/favn_authoring"),
      internal_dep(:favn_local, "apps/favn_local"),
      internal_dep(:favn_orchestrator, "apps/favn_orchestrator", only: :test),
      internal_dep(:favn_test_support, "apps/favn_test_support", only: :test)
    ]
  end

  defp internal_dep(app, sparse_path, opts \\ []) do
    source =
      if Mix.Project.umbrella?() do
        [in_umbrella: true]
      else
        favn_git_dep(sparse_path)
      end

    {app, Keyword.merge(source, opts)}
  end

  defp favn_git_dep(sparse_path) do
    [
      git: System.get_env("FAVN_GIT_SOURCE") || "https://github.com/eirhop/favn.git",
      branch: System.get_env("FAVN_GIT_REF") || "main",
      sparse: sparse_path
    ]
  end
end
