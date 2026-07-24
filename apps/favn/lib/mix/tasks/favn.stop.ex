defmodule Mix.Tasks.Favn.Stop do
  use Mix.Task

  @shortdoc "Stops Docker-free local Favn development"

  @moduledoc """
  Stops the local runner, Orchestrator, and View. The command is idempotent and
  never changes PostgreSQL or customer data.
  """

  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.stop", args, root_dir: :string)

    case FavnLocal.stop(opts) do
      :ok -> IO.puts("Favn development stopped")
      {:error, reason} -> Mix.raise("failed to stop Favn development: #{inspect(reason)}")
    end
  end
end
