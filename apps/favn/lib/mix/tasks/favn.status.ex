defmodule Mix.Tasks.Favn.Status do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Shows local Favn dev stack status"

  @moduledoc """
  Shows the local project-scoped Favn dev stack status.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} = OptionParser.parse(args, strict: [root_dir: :string])

    status = Dev.status(opts)

    IO.puts("Favn local dev stack")
    IO.puts("status: #{status.stack_status}")
    IO.puts("storage: #{status.storage}")
    IO.puts("manifest: #{status.active_manifest_version_id || "none"}")
    IO.puts("web: #{format_service(status.services.web)}")
    IO.puts("orchestrator: #{format_service(status.services.orchestrator)}")
    IO.puts("runner: #{format_service(status.services.runner)}")

    case status.stack_status do
      :partial -> IO.puts("hint: run mix favn.stop to clean up partial/dead services")
      :stale -> IO.puts("hint: run mix favn.stop to clear stale runtime state")
      _other -> :ok
    end

    last_failure = if is_map(status.last_failure), do: inspect(status.last_failure), else: "none"
    IO.puts("last failure: #{last_failure}")
  end

  defp format_service(service) do
    pid = service.pid || "n/a"
    "#{service.status} pid=#{pid}"
  end
end
