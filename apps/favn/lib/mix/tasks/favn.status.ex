defmodule Mix.Tasks.Favn.Status do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Shows local Favn dev stack status"

  @moduledoc """
  Shows the local project-scoped Favn dev stack status.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.status", args, root_dir: :string)

    status = Dev.status(opts)

    IO.puts("Favn local dev stack")
    IO.puts("status: #{status.stack_status}")
    IO.puts("storage: #{status.storage}")
    IO.puts("manifest: #{status.active_manifest_version_id || "none"}")
    IO.puts("local URLs:")
    IO.puts("web: #{format_service(status.services.web)} url=#{status.user_urls.web}")

    IO.puts(
      "orchestrator API: #{format_service(status.services.orchestrator)} url=#{status.user_urls.orchestrator_api}"
    )

    IO.puts("internal control plane:")
    IO.puts("runner node: #{format_internal_node(status.internal_control.runner_node)}")

    IO.puts(
      "orchestrator node: #{format_internal_node(status.internal_control.orchestrator_node)}"
    )

    IO.puts("control node: #{format_control_node(status.internal_control.control_node)}")

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

  defp format_internal_node(nil), do: "n/a"

  defp format_internal_node(node) do
    pid = node.pid || "n/a"
    name = node.node_name || "n/a"
    port = node.distribution_port || "n/a"

    "#{node.status} pid=#{pid} node=#{name} distribution_port=#{port}"
  end

  defp format_control_node(node) do
    name = node.node_name || "n/a"
    port = node.distribution_port || "n/a"

    "node=#{name} distribution_port=#{port}"
  end
end
