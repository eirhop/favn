defmodule Mix.Tasks.Favn.Build.Single do
  use Mix.Task

  @shortdoc "Builds the project-local single-node bundle"

  @moduledoc """
  Builds `.favn/build/single/<build_id>` and `.favn/dist/single/<build_id>`.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args, strict: [root_dir: :string, storage: :string])

    opts = normalize_storage(opts)

    case Dev.build_single(opts) do
      {:ok, %{build_id: build_id, dist_dir: dist_dir}} ->
        IO.puts("Favn single build complete")
        IO.puts("build id: #{build_id}")
        IO.puts("dist: #{dist_dir}")

      {:error, :install_required} ->
        Mix.raise("build blocked: install required; run mix favn.install")

      {:error, :install_stale} ->
        Mix.raise("build blocked: install stale; run mix favn.install --force")

      {:error, {:invalid_storage, value}} ->
        Mix.raise(
          "single build failed: invalid storage #{inspect(value)}; expected sqlite|postgres"
        )

      {:error, {:unsupported_root_dir, requested, current}} ->
        Mix.raise(
          "single build is rooted in the current Mix project only; got --root-dir=#{requested}, current=#{current}"
        )

      {:error, reason} ->
        Mix.raise("single build failed: #{inspect(reason)}")
    end
  end

  defp normalize_storage(opts) do
    case Keyword.get(opts, :storage) do
      nil -> opts
      "sqlite" -> Keyword.put(opts, :storage, :sqlite)
      "postgres" -> Keyword.put(opts, :storage, :postgres)
      other -> Keyword.put(opts, :storage, other)
    end
  end
end
