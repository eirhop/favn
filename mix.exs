defmodule Flux.MixProject do
  use Mix.Project

  def project do
    [
      app: :flux,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :phoenix_pubsub],
      mod: {Flux.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub,
       git: "https://github.com/phoenixframework/phoenix_pubsub.git", tag: "v2.2.0"}
    ]
  end
end
