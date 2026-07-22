Code.require_file("rel/control_plane/release.exs", __DIR__)

defmodule FavnUmbrella.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      apps: [
        :favn,
        :favn_authoring,
        :favn_azure,
        :favn_core,
        :favn_duckdb,
        :favn_duckdb_adbc,
        :favn_local,
        :favn_orchestrator,
        :favn_runner,
        :favn_sql_runtime,
        :favn_storage_postgres,
        :favn_test_support,
        :favn_view
      ],
      version: "0.5.0-dev",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: FavnControlPlane.Release.config(),
      aliases: aliases(),
      listeners: listeners(Mix.env()),
      dialyzer: [
        plt_add_apps: [:mix],
        ignore_warnings: ".dialyzer_ignore.exs",
        list_unused_filters: true
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        test: :test,
        "test.acceptance": :test,
        "test.container": :test,
        "test.slow": :test
      ]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir,
       github: "jeremyjh/dialyxir",
       ref: "3553678f4d69281ac6db61034bcf35bcb30cfd78",
       only: [:dev, :test],
       runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false}
    ]
  end

  defp listeners(:dev), do: [Phoenix.CodeReloader]
  defp listeners(_env), do: []

  defp aliases do
    [
      test: &test/1,
      "test.acceptance": [
        "do --app favn_local cmd mix test --no-compile --only acceptance --exclude container --timeout 1200000",
        "do --app favn_view cmd mix test --no-compile --only browser --timeout 1200000"
      ],
      "test.container": [
        "do --app favn_local cmd mix test --no-compile --only container --timeout 1200000"
      ],
      "test.slow": [
        "do --app favn cmd mix test --no-compile --only slow --timeout 1200000",
        "do --app favn_local cmd mix test --no-compile --only slow --timeout 1200000",
        "do --app favn_storage_postgres cmd mix test --no-compile --only slow --timeout 1200000"
      ]
    ]
  end

  defp test(args) do
    elixir =
      System.find_executable("elixir") ||
        Mix.raise("could not find the elixir executable required by the umbrella test runner")

    case System.cmd(elixir, ["scripts/test_umbrella.exs" | args],
           into: IO.stream(:stdio, :line),
           stderr_to_stdout: true
         ) do
      {_stream, 0} ->
        :ok

      {_stream, status} ->
        Mix.raise("umbrella fast tests failed (status=#{status})")
    end
  end
end
