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
      extra_applications: [:logger, :phoenix_pubsub],
      mod: {Favn.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
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
