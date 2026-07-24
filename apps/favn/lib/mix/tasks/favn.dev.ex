defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @shortdoc "Starts Docker-free local Favn development"

  @moduledoc """
  Starts the Orchestrator and View in the current development BEAM and one
  separate runner BEAM using the consumer's compiled code.

  PostgreSQL must already be running, migrated, and provisioned. Environment
  variables must be loaded before invoking the task.

      mix favn.dev
      mix favn.dev --scheduler
  """

  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["app.config"]

  @impl Mix.Task
  def run(args) do
    opts =
      CLIArgs.parse_no_args!("favn.dev", args,
        root_dir: :string,
        scheduler: :boolean
      )

    Mix.Task.run("compile")

    case FavnLocal.dev(opts) do
      {:ok, summary} ->
        IO.puts("Favn development is ready")
        IO.puts("View: #{summary.view_url}")
        IO.puts("Orchestrator: #{summary.orchestrator_url}")
        IO.puts("Workspace: #{summary.workspace_id}")
        IO.puts("Runner release: #{summary.runner_release_id}")
        IO.puts("Local administrator: admin")
        IO.puts("Local administrator password is stored in .favn/local/credentials.json")
        FavnLocal.await_shutdown(summary.supervisor)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp error_message({:missing_env, name}),
    do: "missing required environment variable #{name}"

  defp error_message({:postgres_schema_not_ready, command}),
    do: "PostgreSQL schema is not ready; run #{command}"

  defp error_message({:workspace_not_found, workspace_id, command}),
    do: "workspace #{workspace_id} is not provisioned; run #{command}"

  defp error_message({:legacy_local_state, path}),
    do: "obsolete Docker-era local state exists at #{path}; remove that generated directory once"

  defp error_message(reason), do: "failed to start Favn development: #{inspect(reason)}"
end
