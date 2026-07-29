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

    build_view_assets!()
    recompile_view!()
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

  defp error_message({:invalid_env, name, expected}),
    do: "invalid #{name}; expected #{expected}"

  defp error_message({:legacy_local_state, path}),
    do: "obsolete Docker-era local state exists at #{path}; remove that generated directory once"

  defp error_message({:runner_exited_before_ready, log_path}),
    do: "the runner exited before it was ready; its output is in #{log_path}"

  defp error_message({:runner_start_timeout, log_path}) do
    "the runner did not become ready in time; its output is in #{log_path}. " <>
      "Set FAVN_DEV_RUNNER_START_TIMEOUT_MS to allow longer on a slow filesystem."
  end

  defp error_message(reason), do: "failed to start Favn development: #{inspect(reason)}"

  defp build_view_assets! do
    case Mix.Project.deps_paths() do
      %{favn_view: view_root} ->
        FavnLocal.Assets.build!(view_root)
        verify_stylesheet!(view_root)

      _missing ->
        Mix.raise("failed to locate favn_view source assets")
    end
  end

  # The View links the built stylesheet at runtime, so a missing or empty file
  # produces a silently unstyled page. Fail here, where the cause is obvious.
  defp verify_stylesheet!(view_root) do
    path = Path.join(view_root, "priv/static/assets/css/app.css")

    case File.stat(path) do
      {:ok, %File.Stat{size: size}} when size > 0 -> :ok
      _other -> Mix.raise("favn_view stylesheet was not built: #{path}")
    end
  end

  defp recompile_view! do
    mix = System.find_executable("mix") || Mix.raise("mix executable not found")

    case System.cmd(mix, ["deps.compile", "favn_view"],
           into: IO.stream(:stdio, :line),
           stderr_to_stdout: true
         ) do
      {_output, 0} ->
        :ok

      {_output, status} ->
        Mix.raise("failed to recompile favn_view after asset generation (exit #{status})")
    end
  end

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
