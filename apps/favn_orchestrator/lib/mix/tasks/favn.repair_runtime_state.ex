defmodule Mix.Tasks.Favn.RepairRuntimeState do
  @moduledoc """
  Repairs stale runtime state left by stopped or crashed orchestrators.

      mix favn.repair_runtime_state --dry-run
      mix favn.repair_runtime_state --apply --run-id run_123

  By default the task only reports planned repair actions. Pass `--apply` to
  mutate runtime state. Filters can narrow repair to one run, one backfill group,
  or rows updated after a timestamp:

      mix favn.repair_runtime_state --dry-run --backfill-id run_parent
      mix favn.repair_runtime_state --apply --since 2026-05-20T00:00:00Z

  The repair workflow expires stale execution leases and materialization claims,
  terminalizes orphaned active runs and active step events, reprojects affected
  backfill parents, and conservatively rebuilds missing freshness state for
  successful independent node results.
  """

  use Mix.Task

  alias FavnOrchestrator.Repair.RuntimeState

  @shortdoc "Repairs stale Favn runtime state"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    with {:ok, opts} <- parse_args(args),
         {:ok, report} <- RuntimeState.repair(opts) do
      print_report(report)
    else
      {:error, %FavnOrchestrator.Repair.Report{} = report} ->
        print_report(report)
        Mix.raise("runtime-state repair completed with errors")

      {:error, reason} ->
        Mix.raise("invalid repair options: #{inspect(reason)}")
    end
  end

  defp parse_args(args) do
    {opts, _argv, invalid} =
      OptionParser.parse(args,
        strict: [
          dry_run: :boolean,
          apply: :boolean,
          run_id: :string,
          backfill_id: :string,
          since: :string
        ],
        aliases: [r: :run_id]
      )

    cond do
      invalid != [] ->
        {:error, {:invalid_options, invalid}}

      Keyword.get(opts, :dry_run, false) and Keyword.get(opts, :apply, false) ->
        {:error, :dry_run_and_apply_are_mutually_exclusive}

      true ->
        with {:ok, since} <- parse_since(Keyword.get(opts, :since)) do
          {:ok,
           [
             dry_run: not Keyword.get(opts, :apply, false),
             run_id: Keyword.get(opts, :run_id),
             backfill_id: Keyword.get(opts, :backfill_id),
             since: since
           ]}
        end
    end
  end

  defp parse_since(nil), do: {:ok, nil}

  defp parse_since(value) do
    case DateTime.from_iso8601(value) do
      {:ok, since, _offset} -> {:ok, since}
      {:error, reason} -> {:error, {:invalid_since, reason}}
    end
  end

  defp print_report(report) do
    Mix.shell().info("runtime-state repair mode=#{report.mode}")
    Mix.shell().info("runs_scanned=#{report.runs_scanned}")
    Mix.shell().info("runs_terminalized=#{report.runs_terminalized}")
    Mix.shell().info("steps_terminalized=#{report.steps_terminalized}")
    Mix.shell().info("execution_leases_expired=#{report.execution_leases_expired}")
    Mix.shell().info("materialization_claims_expired=#{report.materialization_claims_expired}")
    Mix.shell().info("backfill_parents_reprojected=#{report.backfill_parents_reprojected}")
    Mix.shell().info("freshness_states_rebuilt=#{report.freshness_states_rebuilt}")
    Mix.shell().info("freshness_states_skipped=#{report.freshness_states_skipped}")

    if report.errors != [] do
      Mix.shell().error("errors=#{inspect(report.errors)}")
    end
  end
end
