defmodule Mix.Tasks.Favn.Stop do
  use Mix.Task

  @shortdoc "Stops local Favn dev stack"

  @moduledoc """
  Stops local project-scoped Favn runtime services.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.stop", args, root_dir: :string)

    case Dev.stop(opts) do
      :ok -> IO.puts("Favn local stack stopped")
      {:error, reason} -> Mix.raise("failed to stop local stack: #{inspect(reason)}")
    end
  end
end
