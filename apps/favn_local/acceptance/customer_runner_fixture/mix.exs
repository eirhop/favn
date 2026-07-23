defmodule FavnRunnerAcceptanceCustomer.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_runner_acceptance_customer,
      version: "0.1.0",
      elixir: "~> 1.20",
      deps: [{:favn, path: "../../../favn"}]
    ]
  end

  def application, do: []
end
