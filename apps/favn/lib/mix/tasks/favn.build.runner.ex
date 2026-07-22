defmodule Mix.Tasks.Favn.Build.Runner do
  use Mix.Task

  @requirements ["app.config"]
  @shortdoc "Builds the project-local runner artifact"

  @moduledoc """
  Builds `.favn/dist/runner/<runner_release_id>` as a relocatable OCI context.

  The runner build is rooted in the current Mix project. `--root-dir` controls
  artifact location only when it matches the current project root.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs
  alias Mix.Tasks.Favn.ProductionBuild

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.build.runner", args, root_dir: :string)

    ProductionBuild.run("favn.build.runner", args, fn -> run_build(opts) end)
  end

  @doc false
  def run_build(opts) when is_list(opts) do
    case Dev.build_runner(opts) do
      {:ok,
       %{
         runner_release_id: release_id,
         dist_dir: dist_dir,
         manifest_dir: manifest_dir,
         status: status
       }} ->
        IO.puts("Favn runner build complete")
        IO.puts("runner release: #{release_id}")
        IO.puts("status: #{status}")
        IO.puts("dist: #{dist_dir}")
        IO.puts("aligned manifest: #{manifest_dir}")
        IO.puts("note: see #{Path.join(dist_dir, "operator-notes.md")}")

      {:error, :install_required} ->
        Mix.raise("build blocked: install required; run mix favn.install")

      {:error, :install_stale} ->
        Mix.raise(
          "build blocked: install stale; run mix favn.install to refresh, or mix favn.install --force to rebuild"
        )

      {:error, {:missing_tool, tool}} ->
        Mix.raise("build blocked: missing required tool #{tool}; run mix favn.install")

      {:error, {:unsupported_root_dir, requested, current}} ->
        Mix.raise(
          "runner build is rooted in the current Mix project only; got --root-dir=#{requested}, current=#{current}"
        )

      {:error, reason} ->
        Mix.raise("runner build failed: #{inspect(reason)}")
    end
  end
end
