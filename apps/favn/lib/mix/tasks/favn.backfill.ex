defmodule Mix.Tasks.Favn.Backfill do
  use Mix.Task

  @shortdoc "Submits and inspects local Favn operational backfills"

  @moduledoc """
  Submits and inspects operational backfills in the running local Favn dev stack.

      mix favn.backfill submit MyApp.Pipelines.Daily --from 2026-04-01 --to 2026-04-07 --kind day
      mix favn.backfill submit MyApp.Pipelines.Daily --window month:2025-05..2026-05 --refresh force
      mix favn.backfill missing-plan MyApp.Assets.Orders --plan-file coverage-plan.json
      mix favn.backfill missing-submit MyApp.Assets.Orders --plan-file coverage-plan.json
      mix favn.backfill windows RUN_ID

  The local CLI submit path accepts explicit `--from`/`--to`/`--kind` ranges or
  compact `--window kind:FROM..TO` syntax for `hour`, `day`, `month`, and `year`
  windows. Use `--dry-run` to print the resolved windows without creating runs.
  By default `submit` waits for the parent backfill run to finish. Use
  `--no-wait` to return after submission. Use `--wait-timeout-ms` for local
  polling and `--run-timeout-ms` for child run execution timeout.
  `--retry-max-attempts` and `--retry-backoff-ms` apply one fixed-backoff
  operator policy to every child run; authored per-asset overrides are replaced
  because operator policy has highest precedence.

  `submit --refresh force` intentionally recomputes selected windows even when
  stored freshness says they are already successful.
  """

  alias Favn.CLI
  alias Favn.CLI.Error

  @submit_switches [
    root_dir: :string,
    from: :string,
    to: :string,
    kind: :string,
    window: :string,
    dry_run: :boolean,
    timezone: :string,
    refresh: :string,
    wait: :boolean,
    wait_timeout_ms: :integer,
    run_timeout_ms: :integer,
    retry_max_attempts: :integer,
    retry_backoff_ms: :integer,
    timeout_ms: :integer,
    poll_interval_ms: :integer
  ]

  @windows_switches [
    root_dir: :string,
    status: :string,
    limit: :integer,
    cursor: :string
  ]
  @missing_plan_switches [
    root_dir: :string,
    plan_file: :string,
    cursor: :string,
    limit: :integer
  ]
  @missing_submit_switches [root_dir: :string, plan_file: :string]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:submit, pipeline_module, opts}} ->
        submit(pipeline_module, opts)

      {:ok, {:missing_plan, asset, opts}} ->
        plan_missing(asset, opts)

      {:ok, {:missing_submit, asset, opts}} ->
        submit_missing(asset, opts)

      {:ok, {:windows, run_id, opts}} ->
        list_windows(run_id, opts)

      {:error, message} ->
        Mix.raise(message)
    end
  end

  @doc false
  def parse_args(["submit" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @submit_switches)

    cond do
      invalid != [] ->
        {:error, "invalid option for mix favn.backfill submit"}

      rest == [] ->
        {:error, "missing pipeline module; usage: #{submit_usage()}"}

      length(rest) > 1 ->
        {:error, "expected one pipeline module; usage: #{submit_usage()}"}

      mixed_submit_range_flags?(opts) ->
        {:error, "--window cannot be combined with --from, --to, or --kind"}

      missing_submit_opts(opts) != [] ->
        {:error, "missing required option(s): #{join_options(missing_submit_opts(opts))}"}

      true ->
        [pipeline_module] = rest
        {:ok, {:submit, pipeline_module, with_default_timezone(opts)}}
    end
  end

  def parse_args(["windows" | args]) do
    parse_one_id_command(args, @windows_switches, :windows, "RUN_ID")
  end

  def parse_args(["missing-plan" | args]) do
    parse_one_id_command(args, @missing_plan_switches, :missing_plan, "ASSET")
  end

  def parse_args(["missing-submit" | args]) do
    with {:ok, {:missing_submit, asset, opts}} <-
           parse_one_id_command(args, @missing_submit_switches, :missing_submit, "ASSET"),
         plan_file when is_binary(plan_file) and plan_file != "" <-
           Keyword.get(opts, :plan_file) do
      {:ok, {:missing_submit, asset, opts}}
    else
      nil -> {:error, "missing required option: --plan-file"}
      "" -> {:error, "missing required option: --plan-file"}
      {:error, _message} = error -> error
    end
  end

  def parse_args([]), do: {:error, "missing subcommand; usage: #{usage()}"}

  def parse_args([unknown | _args]),
    do: {:error, "unknown subcommand #{inspect(unknown)}; usage: #{usage()}"}

  defp submit(pipeline_module, opts) do
    case CLI.submit_backfill(pipeline_module, opts) do
      {:ok, run_or_plan} ->
        if Keyword.get(opts, :dry_run, false) do
          print_plan("Backfill dry run", run_or_plan)
        else
          print_run("Submitted pipeline backfill", run_or_plan)
        end

      {:error, {:run_failed, run}} ->
        print_run("Submitted pipeline backfill", run)

        Mix.raise(
          "backfill parent run finished with status #{run["status"] || inspect(run[:status])}"
        )

      {:error, {:run_failed, message, run}} ->
        print_run("Submitted pipeline backfill", run)
        Mix.raise(message)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp plan_missing(asset, opts) do
    case CLI.plan_missing_asset_backfill(asset, opts) do
      {:ok, plan} ->
        print_missing_plan("Missing-window backfill plan", plan)
        maybe_write_plan(plan, Keyword.get(opts, :plan_file))

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  defp submit_missing(asset, opts) do
    with {:ok, plan} <- read_plan(Keyword.fetch!(opts, :plan_file)) do
      print_missing_plan("Submitting missing-window backfill plan", plan)

      case CLI.submit_missing_asset_backfill(asset, plan, opts) do
        {:ok, run_id} -> IO.puts("run: #{run_id}")
        {:error, reason} -> Mix.raise(error_message(reason))
      end
    else
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp list_windows(run_id, opts) do
    case CLI.list_backfill_windows(run_id, opts) do
      {:ok, page} -> print_page("Backfill windows", page)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp parse_one_id_command(args, switches, command, id_label) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)

    case {invalid, rest} do
      {[], [id]} ->
        {:ok, {command, id, opts}}

      {[], []} ->
        {:error,
         "missing #{id_label}; usage: mix favn.backfill #{command_name(command)} #{id_label}"}

      {[], _many} ->
        {:error,
         "expected one #{id_label}; usage: mix favn.backfill #{command_name(command)} #{id_label}"}

      {_invalid, _rest} ->
        {:error, "invalid option for mix favn.backfill #{command_name(command)}"}
    end
  end

  defp missing_submit_opts(opts) do
    if Keyword.get(opts, :window) not in [nil, ""] do
      []
    else
      [:from, :to, :kind]
      |> Enum.reject(fn key -> Keyword.get(opts, key) not in [nil, ""] end)
      |> Enum.map(&option_name/1)
    end
  end

  defp mixed_submit_range_flags?(opts) do
    Keyword.get(opts, :window) not in [nil, ""] and
      Enum.any?([:from, :to, :kind], &(Keyword.get(opts, &1) not in [nil, ""]))
  end

  defp with_default_timezone(opts), do: Keyword.put_new(opts, :timezone, "Etc/UTC")

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message({:pipeline_not_found, requested, available}),
    do: pipeline_not_found_message(requested, available)

  defp error_message({:run_wait_timeout, run_id}),
    do: "timed out waiting for backfill parent run #{run_id}"

  defp error_message({:backfill_wait_timeout, backfill_id}),
    do: "timed out waiting for backfill #{backfill_id}"

  defp error_message({:backfill_failed, backfill}),
    do: "backfill #{backfill["backfill_id"] || "unknown"} finished with status failed"

  defp error_message({:invalid_option, :timeout_ms}), do: "--timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :wait_timeout_ms}),
    do: "--wait-timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :run_timeout_ms}),
    do: "--run-timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :poll_interval_ms}),
    do: "--poll-interval-ms must be greater than 0"

  defp error_message({:invalid_option, :retry_max_attempts}),
    do: "--retry-max-attempts must be greater than 0 and includes the initial attempt"

  defp error_message({:invalid_option, :retry_backoff_ms}),
    do: "--retry-backoff-ms must be 0 or greater"

  defp error_message({:invalid_window_range, _value}),
    do: "--window must use KIND:FROM..TO syntax, for example month:2025-05..2026-05"

  defp error_message(:mixed_window_range_options),
    do: "--window cannot be combined with --from, --to, or --kind"

  defp error_message({:orchestrator_validation_failed, message}), do: Error.safe_message(message)
  defp error_message({:plan_file_read_failed, reason}), do: "could not read plan file: #{reason}"
  defp error_message(:invalid_coverage_plan_file), do: "plan file does not contain a JSON object"
  defp error_message(:missing_coverage_requires_asset), do: "target must be an asset"

  defp error_message(reason),
    do:
      Error.format(reason,
        context: "backfill",
        next: "inspect the plan or run with mix favn.backfill status ID"
      )

  defp print_run(title, run) do
    IO.puts(title)
    IO.puts("manifest: #{run["manifest_version_id"] || "unknown"}")
    if run["backfill_id"], do: IO.puts("backfill: #{run["backfill_id"]}")
    IO.puts("run: #{run["id"] || run["root_run_id"] || "unknown"}")
    IO.puts("status: #{run["status"] || "unknown"}")
  end

  defp print_items(title, items) do
    IO.puts(title)
    IO.puts("count: #{length(items)}")

    Enum.each(items, fn item ->
      IO.puts(JSON.encode!(item))
    end)
  end

  defp print_page(title, %{"items" => items, "pagination" => pagination}) when is_list(items) do
    print_items(title, items)

    if Map.get(pagination, "has_more") do
      IO.puts("next page: pass --cursor #{Map.fetch!(pagination, "next_cursor")}")
    end
  end

  defp print_plan(title, plan) when is_map(plan) do
    IO.puts(title)
    IO.puts("manifest: #{plan["manifest_version_id"] || "unknown"}")
    IO.puts("target: #{plan["target_id"] || "unknown"}")
    IO.puts("pipeline: #{plan["pipeline_module"] || "unknown"}")
    IO.puts("kind: #{plan["kind"] || "unknown"}")
    IO.puts("timezone: #{plan["timezone"] || "unknown"}")
    IO.puts("windows: #{plan["window_count"] || 0}")
    IO.puts("range_start_at: #{plan["range_start_at"] || "unknown"}")
    IO.puts("range_end_at: #{plan["range_end_at"] || "unknown"}")

    plan
    |> Map.get("window_keys", [])
    |> Enum.each(&IO.puts("window: #{&1}"))
  end

  defp print_missing_plan(title, plan) when is_map(plan) do
    IO.puts(title)
    IO.puts("plan: #{plan["plan_id"] || "unknown"}")
    IO.puts("hash: #{plan["plan_hash"] || "unknown"}")
    IO.puts("manifest: #{plan["manifest_version_id"] || "unknown"}")
    IO.puts("target: #{plan["target_id"] || "unknown"}")
    IO.puts("evaluated_at: #{plan["evaluated_at"] || "unknown"}")
    IO.puts("coverage checksum: #{plan["evaluation_checksum"] || "unknown"}")
    IO.puts("windows: #{plan["window_count"] || 0}")

    plan
    |> Map.get("windows", [])
    |> Enum.each(&IO.puts("window: #{&1["window_key"]}"))
  end

  defp maybe_write_plan(_plan, nil), do: :ok

  defp maybe_write_plan(plan, path) do
    case File.write(path, JSON.encode!(plan) <> "\n") do
      :ok -> IO.puts("saved: #{path}")
      {:error, reason} -> Mix.raise("could not write plan file: #{:file.format_error(reason)}")
    end
  end

  defp read_plan(path) do
    with {:ok, encoded} <- File.read(path),
         {:ok, plan} when is_map(plan) <- JSON.decode(encoded) do
      {:ok, plan}
    else
      {:error, reason} when is_atom(reason) ->
        {:error, {:plan_file_read_failed, :file.format_error(reason)}}

      _invalid ->
        {:error, :invalid_coverage_plan_file}
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

  defp command_name(:missing_plan), do: "missing-plan"
  defp command_name(:missing_submit), do: "missing-submit"
  defp command_name(command), do: Atom.to_string(command)

  defp join_options(options), do: Enum.join(options, ", ")
  defp option_name(key), do: "--" <> (key |> Atom.to_string() |> String.replace("_", "-"))

  defp usage do
    "mix favn.backfill submit|missing-plan|missing-submit|windows"
  end

  defp submit_usage do
    "mix favn.backfill submit MyApp.Pipelines.Daily --from YYYY-MM-DD --to YYYY-MM-DD --kind day | --window day:YYYY-MM-DD..YYYY-MM-DD"
  end
end
