defmodule Mix.Tasks.Favn.Runs do
  use Mix.Task

  @shortdoc "Lists and inspects local Favn runs"

  @moduledoc """
  Lists and inspects persisted runs in the running local Favn dev stack.

      mix favn.runs list
      mix favn.runs list --status error --limit 20
      mix favn.runs show RUN_ID
  """

  alias Favn.Dev

  @list_switches [root_dir: :string, status: :string, limit: :integer]
  @show_switches [root_dir: :string]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:list, opts}} -> list_runs(opts)
      {:ok, {:show, run_id, opts}} -> show_run(run_id, opts)
      {:error, message} -> Mix.raise(message)
    end
  end

  @doc false
  def parse_args(["list" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @list_switches)

    case {invalid, rest} do
      {[], []} -> {:ok, {:list, opts}}
      {[], _rest} -> {:error, "unexpected argument for mix favn.runs list"}
      {_invalid, _rest} -> {:error, "invalid option for mix favn.runs list"}
    end
  end

  def parse_args(["show" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @show_switches)

    case {invalid, rest} do
      {[], [run_id]} -> {:ok, {:show, run_id, opts}}
      {[], []} -> {:error, "missing RUN_ID; usage: mix favn.runs show RUN_ID"}
      {[], _many} -> {:error, "expected one RUN_ID; usage: mix favn.runs show RUN_ID"}
      {_invalid, _rest} -> {:error, "invalid option for mix favn.runs show"}
    end
  end

  def parse_args([]), do: {:error, "missing subcommand; usage: #{usage()}"}

  def parse_args([unknown | _args]),
    do: {:error, "unknown subcommand #{inspect(unknown)}; usage: #{usage()}"}

  defp list_runs(opts) do
    case Dev.list_runs(opts) do
      {:ok, runs} -> print_runs(runs)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp show_run(run_id, opts) do
    case Dev.get_run(run_id, opts) do
      {:ok, run} -> IO.puts(JSON.encode!(run))
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp print_runs(runs) do
    IO.puts("Runs")
    IO.puts("count: #{length(runs)}")

    Enum.each(runs, fn run ->
      IO.puts(
        Enum.join(
          [
            "run=#{field(run, "id") || "unknown"}",
            "status=#{field(run, "status") || "unknown"}",
            "target=#{target(run)}",
            "started_at=#{field(run, "started_at") || "n/a"}",
            "finished_at=#{field(run, "finished_at") || "n/a"}"
          ],
          " "
        )
      )
    end)
  end

  defp target(run) do
    case field(run, "target_refs") do
      [first | rest] -> Enum.join([first | rest], ",")
      _other -> "n/a"
    end
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, atom_key(key))

  defp atom_key("id"), do: :id
  defp atom_key("status"), do: :status
  defp atom_key("target_refs"), do: :target_refs
  defp atom_key("started_at"), do: :started_at
  defp atom_key("finished_at"), do: :finished_at

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message(:stack_not_healthy),
    do: "stack not healthy; use mix favn.stop then mix favn.dev"

  defp error_message(reason), do: "run inspection failed: #{inspect(reason)}"

  defp usage, do: "mix favn.runs list|show"
end
