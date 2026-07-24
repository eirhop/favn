defmodule Mix.Tasks.Favn.Run do
  use Mix.Task

  @shortdoc "Submits an asset or pipeline run to the local Favn dev stack"

  @moduledoc """
  Submits an asset or pipeline run to the running local Favn dev stack.

      mix favn.run MyApp.Pipelines.Daily
      mix favn.run MyApp.Assets.RawEvents:events --window month:2026-01
      mix favn.run MyApp.Source.Events:movement --window month:2026-07 \
        --dependencies none --refresh force_selected

  Asset runs accept dependency scope `all` or `none` and refresh mode `auto`,
  `missing`, `force_selected`, `force_selected_upstream`, or `force_all`.
  Pipeline runs do not accept `--dependencies` and accept only `auto`,
  `missing`, or `force_all` refresh. The defaults remain dependency scope `all`
  and refresh mode `auto` when the options are omitted.

  `--dependencies none` is an operator override for targeted repair and local
  validation. It plans only the selected asset, so use it only after confirming
  that the asset's upstream inputs are suitable. `force_selected_upstream`
  requires `--dependencies all`.

  By default the task waits for the run to finish. Use `--no-wait` to return
  after submission. Use `--wait-timeout-ms` for local polling and
  `--run-timeout-ms` for per-asset execution timeout. `--timeout-ms` remains an
  alias for both when the more specific options are not provided.

  `--retry-max-attempts N` applies a run-only operator override; the number
  includes the initial attempt. `--retry-backoff-ms MS` selects fixed backoff.
  These options do not make unsafe or unknown-outcome failures retryable. Use
  the pipeline/asset DSL or API `retry_policy` map for exponential backoff.
  """

  alias Favn.CLI

  @switches [
    root_dir: :string,
    wait: :boolean,
    window: :string,
    timezone: :string,
    dependencies: :string,
    refresh: :string,
    idempotency_key: :string,
    timeout_ms: :integer,
    wait_timeout_ms: :integer,
    run_timeout_ms: :integer,
    retry_max_attempts: :integer,
    retry_backoff_ms: :integer,
    poll_interval_ms: :integer
  ]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest} do
      {[], [pipeline_module]} ->
        run_pipeline(pipeline_module, opts)

      {[], []} ->
        Mix.raise("missing target; usage: mix favn.run MyApp.Pipelines.Daily")

      {[], _many} ->
        Mix.raise("expected one target; usage: mix favn.run MyApp.Pipelines.Daily")

      {_invalid, _rest} ->
        Mix.raise("invalid option for mix favn.run")
    end
  end

  defp run_pipeline(target, opts) do
    case CLI.run(target, opts) do
      {:ok, run} ->
        print_run(run, target)

      {:error, {:run_failed, run}} ->
        print_run(run, target)
        Mix.raise(terminal_run_error_message(run))

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  def error_message({:target_not_found, requested, available}),
    do: target_not_found_message(requested, available)

  def error_message({:pipeline_not_found, requested, available}),
    do: target_not_found_message(requested, available)

  def error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  def error_message({:run_wait_timeout, run_id, timeout_ms}) do
    "local wait timed out after #{timeout_ms}ms while run #{run_id} is still in flight; " <>
      "inspect it with mix favn.runs show #{run_id} or rerun with a larger --wait-timeout-ms"
  end

  def error_message({:invalid_option, :timeout_ms}), do: "--timeout-ms must be greater than 0"

  def error_message({:invalid_option, :wait_timeout_ms}),
    do: "--wait-timeout-ms must be greater than 0"

  def error_message({:invalid_option, :run_timeout_ms}),
    do: "--run-timeout-ms must be greater than 0"

  def error_message({:invalid_option, :poll_interval_ms}),
    do: "--poll-interval-ms must be greater than 0"

  def error_message({:invalid_option, :retry_max_attempts}),
    do: "--retry-max-attempts must be greater than 0 and includes the initial attempt"

  def error_message({:invalid_option, :retry_backoff_ms}),
    do: "--retry-backoff-ms must be 0 or greater"

  def error_message({:invalid_option, :idempotency_key}),
    do: "--idempotency-key must be a non-empty string up to 512 bytes"

  def error_message({:invalid_option, :timezone_without_window}),
    do: "--timezone requires --window"

  def error_message({:invalid_option, :dependencies, _value}),
    do: "--dependencies must be one of: all, none"

  def error_message({:invalid_option, :refresh, _value}),
    do:
      "--refresh must be one of: auto, missing, force_selected, " <>
        "force_selected_upstream, force_all"

  def error_message(:dependencies_only_supported_for_assets),
    do: "--dependencies is only supported for asset targets"

  def error_message({:invalid_pipeline_refresh_mode, _value}),
    do: "pipeline --refresh must be one of: auto, missing, force_all"

  def error_message({:refresh_include_upstream_requires_dependencies, :all}),
    do: "--refresh force_selected_upstream requires --dependencies all"

  def error_message({:invalid_window_request, reason}),
    do: "invalid --window value: #{inspect(reason)}"

  def error_message({:orchestrator_validation_failed, message}), do: message

  def error_message(%{operation: operation, reason: reason}) do
    "orchestrator #{operation_label(operation)} failed: #{format_orchestrator_reason(reason)}"
  end

  def error_message(reason), do: "run failed: #{inspect(reason)}"

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

    case format_run_error(run_error(run)) do
      nil -> base
      error -> base <> ": " <> error
    end
  end

  @doc false
  def format_run_error(nil), do: nil
  def format_run_error("nil"), do: nil
  def format_run_error(error) when is_binary(error), do: error

  def format_run_error(%{"message" => message}) when is_binary(message) and message != "",
    do: message

  def format_run_error(%{message: message}) when is_binary(message) and message != "",
    do: message

  def format_run_error(%{"reason" => reason}) when is_binary(reason) and reason != "",
    do: reason

  def format_run_error(%{reason: reason}) when is_binary(reason) and reason != "",
    do: reason

  def format_run_error(error), do: inspect(error)

  defp run_error(run), do: run["error"] || run[:error]

  defp operation_label(operation) when is_atom(operation),
    do: operation |> Atom.to_string() |> String.replace("_", " ")

  @doc false
  def format_orchestrator_reason({:http_error, status, payload}) do
    message = get_in(payload, ["error", "message"])
    details = get_in(payload, ["error", "details"])

    case message do
      message when is_binary(message) and message != "" ->
        "HTTP #{status}: #{message}" <> format_orchestrator_details(details)

      _other ->
        "HTTP #{status}: #{inspect(payload)}"
    end
  end

  def format_orchestrator_reason(reason), do: inspect(reason)

  defp format_orchestrator_details(%{"reason" => reason}) when is_binary(reason) and reason != "",
    do: " (reason: " <> reason <> ")"

  defp format_orchestrator_details(%{reason: reason}) when is_binary(reason) and reason != "",
    do: " (reason: " <> reason <> ")"

  defp format_orchestrator_details(details) when is_map(details) and map_size(details) > 0,
    do: " (details: " <> inspect(details) <> ")"

  defp format_orchestrator_details(_details), do: ""

  defp print_run(run, target) do
    IO.puts("Submitted run")
    IO.puts("target: #{target}")
    IO.puts("manifest: #{run["manifest_version_id"] || "unknown"}")
    IO.puts("run: #{run["id"] || "unknown"}")
    IO.puts("status: #{run["status"] || "unknown"}")

    case format_run_error(run_error(run)) do
      nil -> :ok
      error -> IO.puts("error: " <> error)
    end
  end

  defp target_not_found_message(requested, available) do
    lines = [
      "target is not present in the active manifest: #{requested}",
      "hint: run mix favn.reload if the target was added or changed after mix favn.dev started"
    ]

    case available do
      [] ->
        Enum.join(lines, "\n")

      _ ->
        Enum.join(lines ++ ["available targets:" | Enum.map(available, &"  - #{&1}")], "\n")
    end
  end
end
