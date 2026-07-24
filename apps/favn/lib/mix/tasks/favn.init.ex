defmodule Mix.Tasks.Favn.Init do
  use Mix.Task

  @shortdoc "Creates an authoring sample or deployment example"

  @moduledoc """
  Creates the DuckDB authoring sample or copies the static, customer-owned
  deployment example.

      mix favn.init --duckdb --sample
      mix favn.init --target deployment

  Deployment files are never overwritten.
  """

  alias Favn.CLI.Init

  @switches [duckdb: :boolean, sample: :boolean, target: :string, root_dir: :string]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] or rest != [] do
      Mix.raise("usage: mix favn.init --duckdb --sample | mix favn.init --target deployment")
    end

    opts =
      case Keyword.get(opts, :target) do
        nil -> opts
        "deployment" -> Keyword.put(opts, :target, :deployment)
        _unsupported -> Mix.raise("the only deployment target is --target deployment")
      end

    case Init.run(opts) do
      {:ok, %{target: :deployment} = result} ->
        IO.puts("Favn deployment example copied to #{result.output}")
        IO.puts("Edit the files for your deployment platform before use")

      {:ok, result} ->
        IO.puts("Favn authoring sample created")
        IO.puts("Pipeline: #{result.pipeline_module}")

      {:error, reason} ->
        Mix.raise("init failed: #{inspect(reason)}")
    end
  end
end
