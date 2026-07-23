defmodule Mix.Tasks.Favn.Diagnostics do
  use Mix.Task

  @shortdoc "Shows local Favn operator diagnostics"

  @moduledoc """
  Separates Docker/image installation, the selected deployment contract,
  Favn-role Compose state, and service-authenticated runtime readiness.

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
    deployment = report["deployment_contract"] || %{}
    runner_inputs = report["runner_inputs"] || %{}
    compose = report["compose"] || %{}

    IO.puts("Favn local Docker diagnostics")
    IO.puts("status: #{report["status"]}")

    IO.puts(
      "docker: #{docker[:server_os]}/#{docker[:server_architecture]} #{docker[:server_version]}"
    )

    IO.puts("compose: #{docker[:compose_version]}")
    IO.puts("project status: #{compose[:stack_status]}")
    IO.puts("control-plane source: #{control["source"]}")
    IO.puts("control-plane image: #{control["image_reference"]}")
    IO.puts("control-plane build: #{control["build_id"]}")

    if control["source"] == "maintainer" do
      dirty = if control["checkout_dirty"], do: "dirty", else: "clean"
      IO.puts("maintainer checkout: #{control["checkout"]}")
      IO.puts("maintainer revision: #{control["checkout_revision"]} (#{dirty})")
    end

    IO.puts("deployment contract: #{deployment["status"]}")

    if deployment["error"], do: IO.puts("deployment error: #{deployment["error"]}")

    if map_size(runner_inputs) > 0 do
      IO.puts(
        "runner inputs: #{runner_inputs["application_count"]} applications, " <>
          "#{runner_inputs["file_count"]} files, #{runner_inputs["total_bytes"]} bytes"
      )

      IO.puts(
        "runner project roots: " <>
          Enum.join(runner_inputs["current_application_roots"] || [], ", ")
      )
    end

    IO.puts("runtime: #{inspect(report["runtime"], limit: 50, printable_limit: 4_096)}")
  end

  defp error_message(reason), do: inspect(reason)
end
