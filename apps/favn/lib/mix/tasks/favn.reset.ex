defmodule Mix.Tasks.Favn.Reset do
  use Mix.Task

  @shortdoc "Deletes project-local Favn state"

  @moduledoc """
  Deletes project-local `.favn/` install/build/runtime artifacts.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: [root_dir: :string])

    case Dev.reset(opts) do
      :ok ->
        IO.puts("Favn local state reset complete")

      {:error, :stack_running} ->
        Mix.raise("reset blocked: stack appears running; run mix favn.stop first")

      {:error, reason} ->
        Mix.raise("reset failed: #{inspect(reason)}")
    end
  end
end
