defmodule Mix.Tasks.Favn.Semantic.Diff do
  use Mix.Task
  @shortdoc "Compares two local semantic artifacts"
  @moduledoc """
  Reports deterministic semantic changes between two finished artifacts.

      mix favn.semantic.diff --from previous/semantic.json --to current/semantic.json

  Reads bounded local files. Does not compile source, deploy, or activate changes.
  """
  alias Favn.Semantic.{Artifact, Catalog}
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.semantic.diff", args, from: :string, to: :string)
    unless opts[:from] && opts[:to], do: Mix.raise("missing --from or --to")

    with {:ok, before} <- Artifact.read(opts[:from]),
         {:ok, after_artifact} <- Artifact.read(opts[:to]) do
      Mix.shell().info(Jason.encode!(Catalog.diff(before, after_artifact), pretty: true))
    else
      {:error, reason} -> Mix.raise("invalid semantic artifact: #{inspect(reason)}")
    end
  end
end
