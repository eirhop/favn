defmodule Mix.Tasks.Favn.Init do
  use Mix.Task

  @shortdoc "Bootstraps local Favn files in a Mix project"

  @moduledoc """
  Bootstraps a local Favn sample in the current Mix project.

      mix favn.init --duckdb --sample
  """

  alias Favn.Dev

  @switches [duckdb: :boolean, sample: :boolean, root_dir: :string]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest} do
      {[], []} -> init(opts)
      {_invalid, _rest} -> Mix.raise("usage: mix favn.init --duckdb --sample")
    end
  end

  defp init(opts) do
    case Dev.init(opts) do
      {:ok, result} ->
        print_result(result)

      {:error, {:missing_required_flags, flags}} ->
        Mix.raise("missing required option(s): #{Enum.map_join(flags, ", ", &"--#{&1}")}")

      {:error, {:missing_mix_project, root_dir}} ->
        Mix.raise("init failed: no mix.exs found under #{root_dir}")

      {:error, reason} ->
        Mix.raise("init failed: #{inspect(reason)}")
    end
  end

  defp print_result(result) do
    IO.puts("Favn local bootstrap complete")
    IO.puts("pipeline: #{result.pipeline_module}")

    print_paths("created", result.created)
    print_paths("already present", result.existing)
    print_paths("updated", result.updated)
    print_paths("left unchanged", result.skipped)

    Enum.each(result.warnings, &IO.puts("warning: #{&1}"))
  end

  defp print_paths(_label, []), do: :ok

  defp print_paths(label, paths) do
    IO.puts("#{label}:")
    Enum.each(paths, &IO.puts("  - #{&1}"))
  end
end
