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
    IO.puts("Favn operator diagnostics")
    IO.puts("status: #{report["status"] || report[:status]}")
    IO.puts("generated_at: #{report["generated_at"] || report[:generated_at]}")
    IO.puts("checks:")

    report
    |> checks()
    |> Enum.each(fn check ->
      IO.puts(
        "- #{check["check"] || check[:check]}: #{check["status"] || check[:status]} - #{check["summary"] || check[:summary]}"
      )
    end)
  end

  defp checks(%{"checks" => checks}) when is_list(checks), do: checks
  defp checks(%{checks: checks}) when is_list(checks), do: checks
  defp checks(_report), do: []

  defp error_message(:stack_not_running), do: "local stack is not running; run mix favn.dev first"

  defp error_message(:stack_not_healthy),
    do: "local stack is not healthy; run mix favn.status for details"

  defp error_message(:missing_service_token), do: "local service token is missing"
  defp error_message(reason), do: inspect(reason)
end
