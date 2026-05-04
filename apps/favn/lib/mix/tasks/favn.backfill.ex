defmodule Mix.Tasks.Favn.Backfill do
  use Mix.Task

  @shortdoc "Submits and inspects local Favn operational backfills"

  @moduledoc """
  Submits and inspects operational backfills in the running local Favn dev stack.

      mix favn.backfill submit MyApp.Pipelines.Daily --from 2026-04-01 --to 2026-04-07 --kind day
      mix favn.backfill windows RUN_ID
      mix favn.backfill coverage-baselines
      mix favn.backfill asset-window-states
      mix favn.backfill rerun-window RUN_ID --window-key day:2026-04-01
      mix favn.backfill repair --pipeline-module MyApp.Pipelines.Daily --apply

  The local CLI submit path currently accepts explicit `--from`/`--to`/`--kind`
  ranges only. By default `submit` waits for the parent backfill run to finish.
  Use `--no-wait` to return after submission. Use `--wait-timeout-ms` for local
  polling and `--run-timeout-ms` for child run execution timeout.
  """

  alias Favn.Dev

  @submit_switches [
    root_dir: :string,
    from: :string,
    to: :string,
    kind: :string,
    timezone: :string,
    coverage_baseline_id: :string,
    wait: :boolean,
    wait_timeout_ms: :integer,
    run_timeout_ms: :integer,
    timeout_ms: :integer,
    poll_interval_ms: :integer
  ]

  @windows_switches [
    root_dir: :string,
    pipeline_module: :string,
    window_key: :string,
    status: :string,
    limit: :integer,
    offset: :integer
  ]

  @coverage_switches [
    root_dir: :string,
    pipeline_module: :string,
    source_key: :string,
    segment_key_hash: :string,
    status: :string,
    limit: :integer,
    offset: :integer
  ]

  @asset_state_switches [
    root_dir: :string,
    pipeline_module: :string,
    window_key: :string,
    status: :string,
    asset_ref_module: :string,
    asset_ref_name: :string,
    limit: :integer,
    offset: :integer
  ]

  @rerun_switches [root_dir: :string, window_key: :string]
  @repair_switches [
    root_dir: :string,
    apply: :boolean,
    backfill_run_id: :string,
    pipeline_module: :string
  ]

  @impl Mix.Task
  def run(args) do
    case parse_args(args) do
      {:ok, {:submit, pipeline_module, opts}} ->
        submit(pipeline_module, opts)

      {:ok, {:windows, run_id, opts}} ->
        list_windows(run_id, opts)

      {:ok, {:coverage_baselines, opts}} ->
        list_coverage_baselines(opts)

      {:ok, {:asset_window_states, opts}} ->
        list_asset_window_states(opts)

      {:ok, {:rerun_window, run_id, opts}} ->
        rerun_window(run_id, opts)

      {:ok, {:repair, opts}} ->
        repair(opts)

      {:error, message} ->
        Mix.raise(message)
    end
  end

  @doc false
  def parse_args(["submit" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @submit_switches)

    case {invalid, rest, missing_submit_opts(opts)} do
      {[], [pipeline_module], []} ->
        {:ok, {:submit, pipeline_module, with_default_timezone(opts)}}

      {[], [_pipeline_module], missing} ->
        {:error, "missing required option(s): #{join_options(missing)}"}

      {[], [], _missing} ->
        {:error, "missing pipeline module; usage: #{submit_usage()}"}

      {[], _many, _missing} ->
        {:error, "expected one pipeline module; usage: #{submit_usage()}"}

      {_invalid, _rest, _missing} ->
        {:error, "invalid option for mix favn.backfill submit"}
    end
  end

  def parse_args(["windows" | args]) do
    parse_one_id_command(args, @windows_switches, :windows, "RUN_ID")
  end

  def parse_args(["coverage-baselines" | args]) do
    parse_no_id_command(args, @coverage_switches, :coverage_baselines)
  end

  def parse_args(["asset-window-states" | args]) do
    parse_no_id_command(args, @asset_state_switches, :asset_window_states)
  end

  def parse_args(["rerun-window" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @rerun_switches)

    case {invalid, rest, Keyword.get(opts, :window_key)} do
      {[], [run_id], window_key} when is_binary(window_key) and window_key != "" ->
        {:ok, {:rerun_window, run_id, opts}}

      {[], [_run_id], _missing} ->
        {:error, "missing required option: --window-key"}

      {[], [], _missing} ->
        {:error, "missing RUN_ID; usage: mix favn.backfill rerun-window RUN_ID --window-key KEY"}

      {[], _many, _window_key} ->
        {:error,
         "expected one RUN_ID; usage: mix favn.backfill rerun-window RUN_ID --window-key KEY"}

      {_invalid, _rest, _window_key} ->
        {:error, "invalid option for mix favn.backfill rerun-window"}
    end
  end

  def parse_args(["repair" | args]) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @repair_switches)

    case {invalid, rest, repair_scope_count(opts)} do
      {[], [], count} when count <= 1 ->
        {:ok, {:repair, opts}}

      {[], [], _count} ->
        {:error, "expected at most one repair scope: --backfill-run-id or --pipeline-module"}

      {[], _many, _count} ->
        {:error, "unexpected argument for mix favn.backfill repair"}

      {_invalid, _rest, _count} ->
        {:error, "invalid option for mix favn.backfill repair"}
    end
  end

  def parse_args([]), do: {:error, "missing subcommand; usage: #{usage()}"}

  def parse_args([unknown | _args]),
    do: {:error, "unknown subcommand #{inspect(unknown)}; usage: #{usage()}"}

  defp submit(pipeline_module, opts) do
    case Dev.submit_backfill(pipeline_module, opts) do
      {:ok, run} ->
        print_run("Submitted pipeline backfill", run)

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

  defp list_windows(run_id, opts) do
    case Dev.list_backfill_windows(run_id, opts) do
      {:ok, page} -> print_page("Backfill windows", page)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp list_coverage_baselines(opts) do
    case Dev.list_coverage_baselines(opts) do
      {:ok, page} -> print_page("Coverage baselines", page)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp list_asset_window_states(opts) do
    case Dev.list_asset_window_states(opts) do
      {:ok, page} -> print_page("Asset window states", page)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp rerun_window(run_id, opts) do
    case Dev.rerun_backfill_window(run_id, Keyword.fetch!(opts, :window_key), opts) do
      {:ok, run} -> print_run("Submitted backfill window rerun", run)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp repair(opts) do
    case Dev.repair_backfill_projections(opts) do
      {:ok, report} -> print_repair_report(report)
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

  defp parse_no_id_command(args, switches, command) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: switches)

    case {invalid, rest} do
      {[], []} ->
        {:ok, {command, opts}}

      {[], _rest} ->
        {:error, "unexpected argument for mix favn.backfill #{command_name(command)}"}

      {_invalid, _rest} ->
        {:error, "invalid option for mix favn.backfill #{command_name(command)}"}
    end
  end

  defp repair_scope_count(opts) do
    Enum.count([:backfill_run_id, :pipeline_module], fn key ->
      value = Keyword.get(opts, key)
      not (is_nil(value) or value == "")
    end)
  end

  defp missing_submit_opts(opts) do
    [:from, :to, :kind]
    |> Enum.reject(fn key -> Keyword.get(opts, key) not in [nil, ""] end)
    |> Enum.map(&option_name/1)
  end

  defp with_default_timezone(opts), do: Keyword.put_new(opts, :timezone, "Etc/UTC")

  defp error_message(:stack_not_running), do: "stack not running; use mix favn.dev"

  defp error_message(:stack_not_healthy),
    do: "stack not healthy; use mix favn.stop then mix favn.dev"

  defp error_message({:pipeline_not_found, requested, available}),
    do: pipeline_not_found_message(requested, available)

  defp error_message({:run_wait_timeout, run_id}),
    do: "timed out waiting for backfill parent run #{run_id}"

  defp error_message({:invalid_option, :timeout_ms}), do: "--timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :wait_timeout_ms}),
    do: "--wait-timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :run_timeout_ms}),
    do: "--run-timeout-ms must be greater than 0"

  defp error_message({:invalid_option, :poll_interval_ms}),
    do: "--poll-interval-ms must be greater than 0"

  defp error_message({:orchestrator_validation_failed, message}), do: message
  defp error_message(reason), do: "backfill failed: #{inspect(reason)}"

  defp print_run(title, run) do
    IO.puts(title)
    IO.puts("manifest: #{run["manifest_version_id"] || "unknown"}")
    IO.puts("run: #{run["id"] || "unknown"}")
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
      IO.puts("next page: pass --offset #{Map.fetch!(pagination, "next_offset")}")
    end
  end

  defp print_repair_report(report) when is_map(report) do
    counts = Map.get(report, "counts", %{})
    IO.puts("Backfill projection repair")
    IO.puts("mode: #{if Map.get(report, "apply"), do: "apply", else: "dry-run"}")
    IO.puts("coverage baselines: #{Map.get(counts, "coverage_baselines", 0)}")
    IO.puts("backfill windows: #{Map.get(counts, "backfill_windows", 0)}")
    IO.puts("asset window states: #{Map.get(counts, "asset_window_states", 0)}")
    IO.puts("skips: #{Map.get(counts, "skips", 0)}")

    report
    |> Map.get("skips", [])
    |> Enum.each(&IO.puts(JSON.encode!(&1)))
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

  defp command_name(:coverage_baselines), do: "coverage-baselines"
  defp command_name(:asset_window_states), do: "asset-window-states"
  defp command_name(command), do: Atom.to_string(command)

  defp join_options(options), do: Enum.join(options, ", ")
  defp option_name(key), do: "--" <> (key |> Atom.to_string() |> String.replace("_", "-"))

  defp usage do
    "mix favn.backfill submit|windows|coverage-baselines|asset-window-states|rerun-window|repair"
  end

  defp submit_usage do
    "mix favn.backfill submit MyApp.Pipelines.Daily --from YYYY-MM-DD --to YYYY-MM-DD --kind day"
  end
end
