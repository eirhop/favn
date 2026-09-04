defmodule Mix.Tasks.Favn.Reload do
  use Mix.Task

  @shortdoc "Compiles and reloads local Favn source changes"

  @moduledoc """
  Incrementally compiles the consumer and activates the aligned manifest.
  Changed compiled code replaces the local runner; manifest-only changes keep
  the current runner; an unchanged source and manifest is a no-op.

  Configuration, environment, PostgreSQL, workspace, port, or plugin changes
  require `mix favn.stop` followed by `mix favn.dev`.
  """

  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["app.config"]

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.reload", args, root_dir: :string)

    Mix.Task.reenable("compile")
    Mix.Task.run("compile")

    case FavnLocal.reload(opts) do
      {:ok, result} ->
        print_result(result)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec print_result(map()) :: :ok
  def print_result(result) do
    classification = Map.get(result, :reload_status)
    IO.puts(status_message(classification))
    IO.puts(freshness_message(classification))
    IO.puts("Runner release: #{result.runner_release_id}")
    IO.puts("Manifest: #{result.manifest_version_id}")
    IO.puts(phase_message(result.phases))
    IO.puts("Reload time: #{result.duration_ms}ms")
  end

  defp error_message(:not_running), do: "Favn development is not running; run mix favn.dev"

  defp error_message(:runner_still_draining),
    do: "reload refused because the previous runner is still draining; wait for it to finish"

  defp error_message({:reload_outcome_unknown, _reason}),
    do: "reload outcome is unknown; run mix favn.stop followed by mix favn.dev before reloading"

  defp error_message({:runs_in_flight, count}),
    do: "reload refused because #{count} admitted operation(s) are still in flight"

  defp error_message(reason), do: "failed to reload Favn development: #{inspect(reason)}"

  defp status_message(:unchanged), do: "Favn source unchanged"
  defp status_message(:manifest_deployed), do: "Favn manifest reloaded"
  defp status_message(:runner_replaced), do: "Favn runner reloaded"
  defp status_message(_unclassified), do: "Favn reload completed"

  defp freshness_message(:unchanged), do: "Freshness unchanged"

  defp freshness_message(_reload_status) do
    "Freshness retained; reload does not rerun assets. " <>
      "Use mix favn.run ASSET --refresh force_selected to recompute one asset."
  end

  defp phase_message(phases) do
    "Phases: build #{phases.manifest_build_ms}ms, packages " <>
      "#{phases.execution_packages_ms}ms, publish #{phases.manifest_publication_ms}ms, " <>
      "activate #{phases.manifest_activation_ms}ms"
  end
end
