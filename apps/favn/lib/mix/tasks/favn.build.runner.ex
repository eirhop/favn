defmodule Mix.Tasks.Favn.Build.Runner do
  use Mix.Task

  @shortdoc "Builds the project-local runner artifact"

  @moduledoc """
  Builds `.favn/build/runner/<build_id>` and `.favn/dist/runner/<build_id>`.

  The runner build is rooted in the current Mix project. `--root-dir` controls
  artifact location only when it matches the current project root.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: [root_dir: :string])

    case Dev.build_runner(opts) do
      {:ok, %{build_id: build_id, dist_dir: dist_dir}} ->
        IO.puts("Favn runner build complete")
        IO.puts("build id: #{build_id}")
        IO.puts("dist: #{dist_dir}")
        IO.puts("note: see #{Path.join(dist_dir, "OPERATOR_NOTES.md")}")

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

      {:error, {:unsupported_root_dir, requested, current}} ->
        Mix.raise(
          "runner build is rooted in the current Mix project only; got --root-dir=#{requested}, current=#{current}"
        )

      {:error, reason} ->
        Mix.raise("runner build failed: #{inspect(reason)}")
    end
  end
end
