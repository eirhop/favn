defmodule Mix.Tasks.Favn.Diagnostics do
  use Mix.Task

  @shortdoc "Shows Orchestrator runtime diagnostics"

  @moduledoc """
  Reads authenticated runtime diagnostics from the configured Orchestrator or
  the currently running Docker-free development process.

      mix favn.diagnostics
      mix favn.diagnostics --json
  """

  alias Favn.CLI
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts = CLIArgs.parse_no_args!("favn.diagnostics", args, root_dir: :string, json: :boolean)

    case CLI.diagnostics(opts) do
      {:ok, report} -> print_report(report, Keyword.get(opts, :json, false))
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp print_report(report, true), do: IO.puts(JSON.encode!(report))

  defp print_report(report, false) do
    IO.puts("Favn runtime diagnostics")
    IO.puts("status: #{report["status"]}")
    IO.puts(inspect(report, pretty: true, limit: 100, printable_limit: 8_192))
  end

  defp error_message(reason), do: inspect(reason)
end
