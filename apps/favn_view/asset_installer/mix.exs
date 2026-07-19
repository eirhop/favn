defmodule FavnView.AssetInstaller.MixProject do
  use Mix.Project

  def project do
    [
      app: :favn_view_asset_installer,
      version: "0.1.0",
      build_path: "../../../_build/asset_installer",
      deps_path: "../../../deps",
      lockfile: "../../../mix.lock",
      elixir: "~> 1.20",
      deps: deps(),
      aliases: aliases()
    ]
  end

  defp deps do
    [
      {:esbuild, "~> 0.10", runtime: false},
      {:tailwind, "~> 0.3", runtime: false}
    ]
  end

  defp aliases do
    [
      "assets.setup": ["tailwind.install --if-missing", "esbuild.install --if-missing"]
    ]
  end
end
