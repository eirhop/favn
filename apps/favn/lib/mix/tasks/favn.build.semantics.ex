defmodule Mix.Tasks.Favn.Build.Semantics do
  use Mix.Task

  @shortdoc "Builds an independent SQL semantic artifact"
  @moduledoc """
  Builds a semantic artifact without booting or deploying a runner.

      MIX_BUILD_PATH=_build_semantic mix favn.build.semantics --output dist/semantics

  Set `MIX_BUILD_PATH` before Mix starts to a dedicated directory.
  Mix can compile dependencies while locating this task, so isolation must already
  exist at process startup. The task checks the path before `app.config`; this
  guard cannot undo earlier Mix bootstrap. Never launch a runner from this path.
  Existing semantic/dependency build output can be reused; symlinks are rejected.
  The DuckDB compiler must be installed; no production connection is used.
  """

  alias FavnAuthoring.Semantic.{BuildDirectory, Builder}
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.build.semantics", args, output: :string)
    unless Keyword.has_key?(opts, :output), do: Mix.raise("missing --output")

    case BuildDirectory.validate(System.get_env("MIX_BUILD_PATH"), File.cwd!()) do
      :ok -> :ok
      {:error, reason} -> Mix.raise("semantic build isolation failed: #{reason}")
    end

    Mix.Task.run("app.config")

    case Builder.build(opts) do
      {:ok, result} ->
        Mix.shell().info("Semantic artifact: #{result.path}")

      {:error, reason} ->
        Mix.raise("semantic build failed: #{inspect(reason, limit: 100, printable_limit: 4096)}")
    end
  end
end
