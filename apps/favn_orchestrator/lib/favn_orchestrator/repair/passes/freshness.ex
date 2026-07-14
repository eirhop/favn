defmodule FavnOrchestrator.Repair.Passes.Freshness do
  @moduledoc """
  Conservative freshness-state repair pass.

  Historical run result shapes do not always contain enough stable freshness
  identity to safely rebuild `AssetFreshnessState`. This pass rebuilds missing
  freshness only for successful planned nodes without upstream dependencies;
  dependent nodes are skipped unless a future repair pass can prove the exact
  consumed input versions.
  """

  alias Favn.Freshness.Key
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.Repair.Report
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @doc "Inspects successful runs and reports conservative freshness repair counts."
  @spec run(Report.t(), keyword()) :: Report.t()
  def run(%Report{} = report, opts) when is_list(opts) do
    case candidate_runs(opts) do
      {:ok, runs} ->
        runs
        |> Enum.filter(&(&1.status == :ok and matches_filters?(&1, opts)))
        |> Enum.reduce(report, &repair_run_freshness/2)

      {:error, reason} ->
        Report.error(report, {:freshness_repair_failed, reason})
    end
  end

  defp candidate_runs(opts) do
    cond do
      is_binary(opts[:run_id]) ->
        case Storage.get_run(opts[:run_id]) do
          {:ok, run} -> {:ok, [run]}
          {:error, _reason} = error -> error
        end

      is_binary(opts[:backfill_id]) ->
        Storage.list_execution_group_runs(opts[:backfill_id])

      true ->
        Storage.list_runs(status: :ok)
    end
  end

  defp repair_run_freshness(%RunState{} = run, %Report{} = report) do
    run
    |> successful_node_results()
    |> Enum.reduce(report, fn result, acc -> repair_node_result(run, result, acc) end)
  end

  defp repair_node_result(%RunState{} = run, result, %Report{} = report) do
    case build_repair_state(run, result) do
      {:ok, %AssetFreshnessState{} = state} ->
        maybe_put_repair_state(report, state)

      :skip ->
        Report.bump(report, :freshness_states_skipped)

      {:error, reason} ->
        Report.error(report, {:freshness_repair_failed, run.id, reason})
    end
  end

  defp build_repair_state(%RunState{plan: %{nodes: nodes}} = run, result) when is_map(nodes) do
    with {:ok, node_key} <- result_node_key(result),
         {:ok, node} <- fetch_repair_node(nodes, node_key),
         :ok <- require_independent_node(node),
         {:ok, version} <- Storage.get_manifest_version(run.manifest_version_id) do
      freshness_key = result_freshness_key(result)

      {:ok,
       StateWriter.build_success_state(
         run,
         version,
         node_key,
         %{freshness_key: freshness_key, reason: :runtime_state_repair},
         %{current_states: %{}}
       )}
    else
      :skip -> :skip
      {:error, :not_found} -> :skip
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_repair_state(%RunState{}, _result), do: :skip

  defp maybe_put_repair_state(%Report{} = report, %AssetFreshnessState{} = state) do
    if existing_success?(state) do
      Report.bump(report, :freshness_states_skipped)
    else
      case report.mode do
        :dry_run ->
          Report.bump(report, :freshness_states_rebuilt)

        :apply ->
          case Storage.put_asset_freshness_state(state) do
            :ok -> Report.bump(report, :freshness_states_rebuilt)
            {:error, reason} -> Report.error(report, {:freshness_state_rebuild_failed, reason})
          end
      end
    end
  end

  defp existing_success?(%AssetFreshnessState{} = state) do
    case Storage.get_asset_freshness_state(
           state.asset_ref_module,
           state.asset_ref_name,
           state.freshness_key
         ) do
      {:ok, %AssetFreshnessState{freshness_version: version, latest_success_at: %DateTime{}}}
      when is_binary(version) and version != "" ->
        true

      _other ->
        false
    end
  end

  defp successful_node_results(%RunState{result: result}) when is_map(result) do
    result
    |> Map.get(:node_results, Map.get(result, "node_results", []))
    |> result_values()
    |> Enum.filter(&(result_status(&1) == :ok))
  end

  defp successful_node_results(%RunState{}), do: []

  defp result_values(results) when is_map(results), do: Map.values(results)
  defp result_values(results) when is_list(results), do: results
  defp result_values(_results), do: []

  defp result_status(result) when is_map(result) do
    case field(result, :status) do
      :ok -> :ok
      "ok" -> :ok
      status -> status
    end
  end

  defp result_status(_result), do: nil

  defp result_node_key(result) when is_map(result) do
    case field(result, :node_key) do
      nil -> :skip
      node_key -> {:ok, node_key}
    end
  end

  defp result_node_key(_result), do: :skip

  defp fetch_repair_node(nodes, node_key) when is_map(nodes) do
    case Map.fetch(nodes, node_key) do
      {:ok, node} -> {:ok, node}
      :error -> :skip
    end
  end

  defp require_independent_node(%{upstream: upstream}) when upstream in [nil, []], do: :ok
  defp require_independent_node(%{upstream: _upstream}), do: :skip
  defp require_independent_node(_node), do: :ok

  defp result_freshness_key(result) when is_map(result) do
    field(result, :freshness_key) || Key.latest()
  end

  defp matches_filters?(%RunState{} = run, opts) do
    matches_run_id?(run, Keyword.get(opts, :run_id)) and
      matches_backfill_id?(run, Keyword.get(opts, :backfill_id)) and
      matches_since?(run, Keyword.get(opts, :since))
  end

  defp matches_run_id?(%RunState{id: run_id}, run_id), do: true
  defp matches_run_id?(_run, nil), do: true
  defp matches_run_id?(_run, _run_id), do: false

  defp matches_backfill_id?(%RunState{} = run, backfill_id) when is_binary(backfill_id) do
    run.id == backfill_id or run.parent_run_id == backfill_id or run.root_run_id == backfill_id
  end

  defp matches_backfill_id?(_run, nil), do: true

  defp matches_since?(%RunState{updated_at: %DateTime{} = updated_at}, %DateTime{} = since),
    do: DateTime.compare(updated_at, since) in [:gt, :eq]

  defp matches_since?(_run, nil), do: true
  defp matches_since?(_run, %DateTime{}), do: false

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
