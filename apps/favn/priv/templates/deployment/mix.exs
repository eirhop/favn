customer_app =
  case System.get_env("FAVN_CUSTOMER_APP") do
    value when is_binary(value) ->
      if Regex.match?(~r/\A[a-z][a-z0-9_]*\z/, value),
        do: String.to_atom(value),
        else: raise("FAVN_CUSTOMER_APP must be a lowercase Mix application name")

    _missing ->
      raise "FAVN_CUSTOMER_APP is required"
  end

Application.put_env(:favn_customer_runner, :customer_app, customer_app, persistent: true)

defmodule FavnCustomerRunner.MixProject do
  use Mix.Project

  @customer_app Application.compile_env(:favn_customer_runner, :customer_app)

  def project do
    [
      app: :favn_customer_runner,
      version: "1.0.0",
      elixir: "~> 1.20",
      config_path: "../../config/config.exs",
      lockfile: "../../mix.lock",
      deps_path: "../../deps",
      build_path: "../../_build/favn_customer_runner",
      start_permanent: Mix.env() == :prod,
      deps: [{@customer_app, path: "../.."}],
      releases: [
        favn_runner: [
          applications: [
            {:favn_customer_runner, :load},
            {@customer_app, :load},
            {:favn_runner, :permanent}
          ],
          include_executables_for: [:unix],
          rel_templates_path: ".",
          strip_beams: true
        ] ++ runtime_config()
      ]
    ]
  end

  def application, do: []

  defp runtime_config do
    path = Path.expand("../../config/runtime.exs", __DIR__)
    if File.regular?(path), do: [runtime_config_path: path], else: []
  end
end
