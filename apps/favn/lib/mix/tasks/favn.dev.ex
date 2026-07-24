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
    print_start()

    case FavnLocal.dev(Keyword.put(opts, :progress_fun, &print_progress/1)) do
      {:ok, summary} ->
        print_ready(summary)
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

  defp error_message({:invalid_env, "FAVN_LOG_LEVEL", expected}),
    do: "invalid FAVN_LOG_LEVEL; expected #{expected}"

  defp error_message({:legacy_local_state, path}),
    do: "obsolete Docker-era local state exists at #{path}; remove that generated directory once"

  defp error_message(reason), do: "failed to start Favn development: #{inspect(reason)}"

  @doc false
  @spec print_start() :: :ok
  def print_start do
    IO.puts("Starting Favn development")
  end

  @doc false
  @spec print_progress(FavnLocal.progress_event()) :: :ok
  def print_progress({:configuration_loaded, summary}) do
    IO.puts("  View: #{summary.view_url} (starting)")
  end

  def print_progress(:postgres_ready), do: IO.puts("  PostgreSQL: ready")
  def print_progress({:manifest_built, _summary}), do: IO.puts("  Manifest: built")

  def print_progress({:orchestrator_ready, summary}) do
    IO.puts("  Orchestrator: #{summary.url}")
  end

  def print_progress({:view_ready, summary}) do
    IO.puts("  View: #{summary.url} (listening)")
  end

  def print_progress(:runner_starting), do: IO.puts("  Runner: starting")

  @doc false
  @spec print_ready(map()) :: :ok
  def print_ready(summary) when is_map(summary) do
    IO.puts("  Runner: ready")
    IO.puts("  Manifest: active")
    IO.puts("")
    IO.puts("Favn development is ready")
    IO.puts("Open Favn: #{summary.view_url}")
    IO.puts("Orchestrator: #{summary.orchestrator_url}")
    IO.puts("Workspace: #{summary.workspace_id}")
    IO.puts("Authentication: automatic (local development)")
    IO.puts("")
    IO.puts("Press Ctrl+C to stop.")
  end
end
