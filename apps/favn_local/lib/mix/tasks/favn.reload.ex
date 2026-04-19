defmodule Mix.Tasks.Favn.Reload do
  use Mix.Task

  @shortdoc "Rebuilds and reloads manifest into running local stack"

  @moduledoc """
  Recompiles the project, rebuilds the manifest, publishes it to orchestrator,
  and activates it without restarting orchestrator.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: [root_dir: :string])

    case Dev.reload(opts) do
      :ok ->
        :ok

      {:error, :stack_not_running} ->
        Mix.raise("stack not running; use mix favn.dev")

      {:error, :stack_not_healthy} ->
        Mix.raise("stack not healthy; use mix favn.stop then mix favn.dev")

      {:error, {:in_flight_runs, run_ids}} ->
        Mix.raise("reload blocked: in-flight runs exist #{inspect(run_ids)}")

      {:error, reason} ->
        Mix.raise("reload failed: #{inspect(reason)}")
    end
  end
end
