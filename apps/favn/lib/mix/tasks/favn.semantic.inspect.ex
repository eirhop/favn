defmodule Mix.Tasks.Favn.Semantic.Inspect do
  use Mix.Task
  @shortdoc "Inspects a local semantic artifact"
  @moduledoc """
  Reads a finished semantic artifact without compiling customer source.

      mix favn.semantic.inspect --artifact path/semantic.json --metric sales.revenue --format json

  Output is bounded by the artifact limits. No catalog or runner is contacted.
  """
  alias Favn.Semantic.{Artifact, Catalog}
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts =
      CLIArgs.parse_no_args!("favn.semantic.inspect", args,
        artifact: :string,
        metric: :string,
        format: :string
      )

    unless opts[:artifact], do: Mix.raise("missing --artifact")
    unless opts[:format] in [nil, "json", "text"], do: Mix.raise("format must be json or text")

    with {:ok, artifact} <- Artifact.read(opts[:artifact]) do
      result = Catalog.inspect(artifact, Keyword.take(opts, [:metric]))

      Mix.shell().info(
        if opts[:format] == "json",
          do: Jason.encode!(result, pretty: true),
          else: inspect(result, pretty: true, limit: :infinity)
      )
    else
      {:error, reason} -> Mix.raise("invalid semantic artifact: #{inspect(reason)}")
    end
  end
end
