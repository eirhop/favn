defmodule Mix.Tasks.Favn.Reload do
  use Mix.Task

  @shortdoc "Compiles changes and restarts the local runner"

  @moduledoc """
  Incrementally compiles the consumer, restarts only the local runner under a
  new release ID, and activates the aligned manifest.

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
        IO.puts("Favn runner reloaded")
        IO.puts("Runner release: #{result.runner_release_id}")
        IO.puts("Manifest: #{result.manifest_version_id}")

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp error_message(:not_running), do: "Favn development is not running; run mix favn.dev"

  defp error_message({:runs_in_flight, count}),
    do: "reload refused because #{count} admitted operation(s) are still in flight"

  defp error_message(reason), do: "failed to reload Favn development: #{inspect(reason)}"
end
