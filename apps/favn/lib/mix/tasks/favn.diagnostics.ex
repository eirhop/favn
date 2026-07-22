defmodule Mix.Tasks.Favn.Diagnostics do
  use Mix.Task

  @shortdoc "Shows local Favn operator diagnostics"

  @moduledoc """
  Shows service-authenticated operator diagnostics from the running local stack.

      mix favn.diagnostics
      mix favn.diagnostics --json
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.diagnostics", args, root_dir: :string, json: :boolean)

    case Dev.diagnostics(opts) do
      {:ok, report} -> print_report(report, Keyword.get(opts, :json, false))
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp print_report(report, true), do: IO.puts(JSON.encode!(report))

  defp print_report(report, false) do
    docker = report["docker"] || %{}
    control = report["control_plane"] || %{}
    compose = report["compose"] || %{}

    IO.puts("Favn local Docker diagnostics")
    IO.puts("status: #{report["status"]}")

    IO.puts(
      "docker: #{docker[:server_os]}/#{docker[:server_architecture]} #{docker[:server_version]}"
    )

    IO.puts("compose: #{docker[:compose_version]}")
    IO.puts("project status: #{compose[:stack_status]}")
    IO.puts("control-plane image: #{control["image_reference"]}")
    IO.puts("control-plane build: #{control["build_id"]}")
    IO.puts("runtime: #{inspect(report["runtime"], limit: 50, printable_limit: 4_096)}")
  end

  defp error_message(:stack_not_running), do: "local stack is not running; run mix favn.dev first"

  defp error_message(:orchestrator_not_running),
    do: "local orchestrator is not running; run mix favn.status for details"

  defp error_message(reason), do: inspect(reason)
end
