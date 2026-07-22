defmodule Mix.Tasks.Favn.Runs do
  use Mix.Task

  @shortdoc "Lists, inspects, and cancels local Favn runs"

  @moduledoc """
  Lists, inspects, and cancels persisted runs in the running local Favn dev stack.

      mix favn.runs list
      mix favn.runs list --status error --limit 20
      mix favn.runs show RUN_ID
      mix favn.runs cancel RUN_ID
      mix favn.runs cancel RUN_ID --wait --wait-timeout-ms 30000

  `cancel` requests cancellation through the local orchestrator HTTP boundary.
  With `--wait`, the task polls only that run until it is terminal or the local
  wait timeout expires.
  """

  alias Favn.Dev

  @list_switches [root_dir: :string, status: :string, limit: :integer]
  @show_switches [root_dir: :string]
  @cancel_switches [
    root_dir: :string,
    wait: :boolean,
    timeout_ms: :integer,
    wait_timeout_ms: :integer,
    poll_interval_ms: :integer
  ]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:list, opts}} -> list_runs(opts)
      {:ok, {:show, run_id, opts}} -> show_run(run_id, opts)
      {:ok, {:cancel, run_id, opts}} -> cancel_run(run_id, opts)
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

  def parse_args(["cancel" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @cancel_switches)

    case {invalid, rest} do
      {[], [run_id]} -> {:ok, {:cancel, run_id, opts}}
      {[], []} -> {:error, "missing RUN_ID; usage: mix favn.runs cancel RUN_ID"}
      {[], _many} -> {:error, "expected one RUN_ID; usage: mix favn.runs cancel RUN_ID"}
      {_invalid, _rest} -> {:error, "invalid option for mix favn.runs cancel"}
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

  defp cancel_run(run_id, opts) do
    case Dev.cancel_run(run_id, opts) do
      {:ok, run_or_result} ->
        print_cancel_result(run_id, run_or_result, Keyword.get(opts, :wait, false))

      {:error, reason} ->
        Mix.raise(cancel_error_message(reason))
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

  defp print_cancel_result(run_id, result, waited?) do
    status = field(result, "status") || if(field(result, "cancelled"), do: "cancel_requested")

    IO.puts("Cancellation requested")
    IO.puts("run: #{field(result, "run_id") || field(result, "id") || run_id}")

    if status do
      IO.puts("status: #{status}")
    end

    unless waited? do
      IO.puts("inspect: mix favn.runs show #{run_id}")
    end
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
  defp atom_key("run_id"), do: :run_id
  defp atom_key("cancelled"), do: :cancelled

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message(reason), do: "run inspection failed: #{inspect(reason)}"

  defp cancel_error_message({:invalid_option, :timeout_ms}),
    do: "--timeout-ms must be greater than 0"

  defp cancel_error_message({:invalid_option, :wait_timeout_ms}),
    do: "--wait-timeout-ms must be greater than 0"

  defp cancel_error_message({:invalid_option, :poll_interval_ms}),
    do: "--poll-interval-ms must be greater than 0"

  defp cancel_error_message({:run_wait_timeout, run_id, timeout_ms}) do
    "local wait timed out after #{timeout_ms}ms while run #{run_id} is still in flight; " <>
      "check status with mix favn.runs show #{run_id} or rerun with a larger --wait-timeout-ms"
  end

  defp cancel_error_message(%{operation: operation, reason: {:http_error, status, payload}}) do
    "orchestrator #{operation_label(operation)} failed: HTTP #{status}: #{http_error_message(payload)}"
  end

  defp cancel_error_message(:stack_not_running), do: error_message(:stack_not_running)
  defp cancel_error_message(reason), do: "run cancellation failed: #{inspect(reason)}"

  defp operation_label(operation) when is_atom(operation),
    do: operation |> Atom.to_string() |> String.replace("_", " ")

  defp http_error_message(%{"error" => %{"message" => message}})
       when is_binary(message) and message != "",
       do: message

  defp http_error_message(payload), do: inspect(payload)

  defp usage, do: "mix favn.runs list|show|cancel"
end
