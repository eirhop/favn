defmodule Mix.Tasks.Favn.Run do
  use Mix.Task

  @shortdoc "Submits a pipeline run to the local Favn dev stack"

  @moduledoc """
  Submits a pipeline run to the running local Favn dev stack.

      mix favn.run MyApp.Pipelines.Daily

  By default the task waits for the run to finish. Use `--no-wait` to return
  after submission.
  """

  alias Favn.Dev

  @switches [
    root_dir: :string,
    wait: :boolean,
    window: :string,
    timezone: :string,
    idempotency_key: :string,
    timeout_ms: :integer,
    poll_interval_ms: :integer
  ]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest} do
      {[], [pipeline_module]} ->
        run_pipeline(pipeline_module, opts)

      {[], []} ->
        Mix.raise("missing pipeline module; usage: mix favn.run MyApp.Pipelines.Daily")

      {[], _many} ->
        Mix.raise("expected one pipeline module; usage: mix favn.run MyApp.Pipelines.Daily")

      {_invalid, _rest} ->
        Mix.raise("invalid option for mix favn.run")
    end
  end

  defp run_pipeline(pipeline_module, opts) do
    case Dev.run_pipeline(pipeline_module, opts) do
      {:ok, run} ->
        print_run(run, pipeline_module)

      {:error, {:run_failed, run}} ->
        print_run(run, pipeline_module)
        Mix.raise(terminal_run_error_message(run))

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp error_message({:pipeline_not_found, requested, available}),
    do: pipeline_not_found_message(requested, available)

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message(:stack_not_healthy),
    do: "stack not healthy; use mix favn.stop then mix favn.dev"

  defp error_message({:run_wait_timeout, run_id, timeout_ms}) do
    "local wait timed out after #{timeout_ms}ms while run #{run_id} is still in flight; " <>
      "check status with mix favn.status or rerun with a larger --timeout-ms"
  end

  defp error_message({:invalid_option, :timeout_ms}), do: "--timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :poll_interval_ms}),
    do: "--poll-interval-ms must be greater than 0"

  defp error_message({:invalid_option, :idempotency_key}),
    do: "--idempotency-key must be a non-empty string up to 512 bytes"

  defp error_message({:invalid_option, :timezone_without_window}),
    do: "--timezone requires --window"

  defp error_message({:invalid_window_request, reason}),
    do: "invalid --window value: #{inspect(reason)}"

  defp error_message({:orchestrator_validation_failed, message}), do: message

  defp error_message(%{operation: operation, reason: reason}) do
    "orchestrator #{operation_label(operation)} failed: #{format_orchestrator_reason(reason)}"
  end

  defp error_message(reason), do: "run failed: #{inspect(reason)}"

  @doc false
  def terminal_run_error_message(run) do
    status = run["status"] || run[:status] || "unknown"
    base = "run finished with status #{status}"

    base =
      if status == "timed_out" do
        base <> " (run execution timeout)"
      else
        base
      end

    case run_error(run) do
      nil -> base
      "nil" -> base
      error -> base <> ": #{error}"
    end
  end

  defp run_error(run), do: run["error"] || run[:error]

  defp operation_label(operation) when is_atom(operation),
    do: operation |> Atom.to_string() |> String.replace("_", " ")

  defp format_orchestrator_reason({:http_error, status, payload}) do
    message = get_in(payload, ["error", "message"])

    case message do
      message when is_binary(message) and message != "" -> "HTTP #{status}: #{message}"
      _other -> "HTTP #{status}: #{inspect(payload)}"
    end
  end

  defp format_orchestrator_reason(reason), do: inspect(reason)

  defp print_run(run, pipeline_module) do
    IO.puts("Submitted pipeline run")
    IO.puts("pipeline: #{pipeline_module}")
    IO.puts("manifest: #{run["manifest_version_id"] || "unknown"}")
    IO.puts("run: #{run["id"] || "unknown"}")
    IO.puts("status: #{run["status"] || "unknown"}")

    case run_error(run) do
      nil -> :ok
      "nil" -> :ok
      error -> IO.puts("error: #{error}")
    end
  end

  defp pipeline_not_found_message(requested, available) do
    lines = [
      "pipeline is not present in the active manifest: #{requested}",
      "hint: run mix favn.reload if the pipeline was added or changed after mix favn.dev started"
    ]

    case available do
      [] -> Enum.join(lines, "\n")
      _ -> Enum.join(lines ++ ["available pipelines:" | Enum.map(available, &"  - #{&1}")], "\n")
    end
  end
end
