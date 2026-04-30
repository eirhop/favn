defmodule Mix.Tasks.Favn.Build.Single do
  use Mix.Task

  @shortdoc "Builds the project-local single-node bundle"

  @moduledoc """
  Builds `.favn/build/single/<build_id>` and `.favn/dist/single/<build_id>`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.build.single", args, root_dir: :string, storage: :string)
    opts = normalize_storage(opts)

    case Dev.build_single(opts) do
      {:ok, %{build_id: build_id, dist_dir: dist_dir}} ->
        IO.puts("Favn single build complete")
        IO.puts("build id: #{build_id}")
        IO.puts("dist: #{dist_dir}")
        IO.puts("note: assembly-only artifact; see #{Path.join(dist_dir, "OPERATOR_NOTES.md")}")

      {:error, :install_required} ->
        Mix.raise("build blocked: install required; run mix favn.install")

      {:error, :install_stale} ->
        Mix.raise(
          "build blocked: install stale; run mix favn.install to refresh, or mix favn.install --force to rebuild"
        )

      {:error, {:missing_tool, tool}} ->
        Mix.raise("build blocked: missing required tool #{tool}; run mix favn.install")

      {:error, {:tool_check_failed, tool, status, output}} ->
        Mix.raise(
          "build blocked: required tool #{tool} check failed (status=#{status}): #{output}; rerun mix favn.install"
        )

      {:error, :missing_install_runtime_input} ->
        Mix.raise(
          "build blocked: install runtime input missing; run mix favn.install to refresh, or mix favn.install --force to rebuild"
        )

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
