defmodule Mix.Tasks.Favn.Status do
  use Mix.Task

  @shortdoc "Shows project-scoped Docker Compose status"

  @moduledoc """
  Shows bounded status for the local PostgreSQL, runner, and control-plane
  containers plus their selected immutable release identities.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.status", args, root_dir: :string)
    status = Dev.status(opts)

    IO.puts("Favn local Docker Compose stack")
    IO.puts("status: #{status.stack_status}")
    IO.puts("project: #{status.compose_project || "not installed"}")

    Enum.each(["postgres", "runner", "control-plane"], fn name ->
      IO.puts("#{name}: #{format_service(status.services[name])}")
    end)

    runner = status.runner || %{}
    IO.puts("runner release: #{runner["runner_release_id"] || "none"}")
    IO.puts("runner image: #{runner["image_id"] || "none"}")
    IO.puts("manifest: #{status.active_manifest_version_id || "none"}")
    IO.puts("runtime: #{inspect(status.runtime, limit: 20, printable_limit: 2_048)}")

    if map_size(status.user_urls) > 0 do
      IO.puts("view: #{status.user_urls.web}")
      IO.puts("private API: #{status.user_urls.orchestrator_api}")
    end

    if status.last_failure, do: IO.puts("last failure: #{inspect(status.last_failure)}")
  end

  defp format_service(nil), do: "stopped"

  defp format_service(service) do
    "#{service.status} health=#{service.health} image=#{service.image || "unknown"}"
  end
end
