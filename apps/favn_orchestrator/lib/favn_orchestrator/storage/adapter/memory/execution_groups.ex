defmodule FavnOrchestrator.Storage.Adapter.Memory.ExecutionGroups do
  @moduledoc """
  Execution-group queries and summary maintenance for the in-memory adapter.

  Group discovery uses the run index maintained by `Memory.Runs`; summary reads
  use the persisted read model just like database-backed adapters.
  """

  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory.Backfills
  alias FavnOrchestrator.Storage.Adapter.Memory.Runs
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.Storage.ExecutionGroupSummary
  alias FavnOrchestrator.Storage.RunQuery

  @doc false
  @spec runs(State.t(), String.t()) :: [RunState.t()]
  def runs(%State{} = state, group_id), do: Runs.group(state, group_id)

  @doc false
  @spec run_ids(State.t(), String.t()) :: [String.t()]
  def run_ids(%State{} = state, group_id), do: Enum.map(runs(state, group_id), & &1.id)

  @doc false
  @spec list(State.t(), keyword()) :: Page.t()
  def list(%State{} = state, opts) do
    page_opts = page_opts(opts)

    state
    |> groups()
    |> Enum.filter(&matches_filters?(&1, opts))
    |> sort(Keyword.get(opts, :sort, :started_desc))
    |> Enum.map(& &1.id)
    |> Enum.drop(Keyword.fetch!(page_opts, :offset))
    |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)
    |> Page.from_fetched(page_opts)
  end

  @doc false
  @spec list_summaries(State.t(), keyword()) :: Page.t()
  def list_summaries(%State{} = state, opts) do
    page_opts = page_opts(opts)

    state.execution_group_summaries
    |> Map.values()
    |> Enum.filter(&matches_summary?(&1, opts))
    |> sort_summaries(Keyword.get(opts, :sort, :started_desc))
    |> Enum.drop(Keyword.fetch!(page_opts, :offset))
    |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)
    |> Page.from_fetched(page_opts)
  end

  @doc false
  @spec rebuild(State.t()) :: {non_neg_integer(), State.t()}
  def rebuild(%State{} = state) do
    group_ids = Runs.group_ids(state)
    {length(group_ids), refresh_many(state, group_ids)}
  end

  @doc false
  @spec refresh(State.t(), RunState.t() | String.t()) :: State.t()
  def refresh(%State{} = state, %RunState{} = run) do
    refresh(state, RunQuery.root_execution_group_id(run))
  end

  def refresh(%State{} = state, group_id) when is_binary(group_id) do
    case ExecutionGroupSummary.build(
           runs(state, group_id),
           Backfills.windows_for_run(state, group_id)
         ) do
      {:ok, summary} ->
        summaries = Map.put(state.execution_group_summaries, group_id, summary)
        %{state | execution_group_summaries: summaries}

      {:error, :empty_execution_group} ->
        summaries = Map.delete(state.execution_group_summaries, group_id)
        %{state | execution_group_summaries: summaries}
    end
  end

  @doc false
  @spec refresh_many(State.t(), [String.t()]) :: State.t()
  def refresh_many(%State{} = state, group_ids) do
    group_ids
    |> Enum.uniq()
    |> Enum.reduce(state, &refresh(&2, &1))
  end

  defp groups(state) do
    Enum.map(Runs.group_ids(state), fn group_id ->
      group_runs = runs(state, group_id)
      root = Enum.find(group_runs, &(&1.id == group_id)) || oldest_run(group_runs)

      %{
        id: group_id,
        root: root,
        runs: group_runs,
        activity: group_runs |> Enum.map(&Runs.sort_key/1) |> Enum.max(fn -> 0 end)
      }
    end)
  end

  defp oldest_run(runs), do: Enum.min_by(runs, &Runs.sort_key/1)

  defp matches_summary?(summary, opts) do
    matches_summary_status?(summary, Keyword.get(opts, :status)) and
      matches_summary_trigger?(summary, Keyword.get(opts, :trigger_type)) and
      matches_summary_target?(summary, Keyword.get(opts, :target_asset)) and
      matches_summary_search?(summary, Keyword.get(opts, :search)) and
      matches_summary_window?(summary, Keyword.get(opts, :window)) and
      matches_summary_only_filters?(summary, opts)
  end

  defp sort_summaries(summaries, :failed_first),
    do:
      Enum.sort_by(summaries, &{if(&1.failure_count > 0, do: 0, else: 1), -summary_activity(&1)})

  defp sort_summaries(summaries, :running_first),
    do: Enum.sort_by(summaries, &{if(&1.active?, do: 0, else: 1), -summary_activity(&1)})

  defp sort_summaries(summaries, :status_priority),
    do: Enum.sort_by(summaries, &{summary_status_priority(&1), -summary_activity(&1)})

  defp sort_summaries(summaries, _sort),
    do: Enum.sort_by(summaries, &summary_activity/1, :desc)

  defp matches_summary_status?(_summary, nil), do: true
  defp matches_summary_status?(summary, status), do: summary.root_status == status

  defp matches_summary_trigger?(_summary, nil), do: true
  defp matches_summary_trigger?(summary, trigger), do: summary.trigger_type == trigger

  defp matches_summary_target?(_summary, nil), do: true
  defp matches_summary_target?(summary, target), do: target in summary.target_assets

  defp matches_summary_search?(_summary, value) when value in [nil, ""], do: true

  defp matches_summary_search?(summary, search) do
    search = String.downcase(to_string(search))

    [summary.id, summary.trigger_type | summary.target_assets]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp matches_summary_window?(_summary, nil), do: true
  defp matches_summary_window?(summary, :has_window), do: summary.total_windows > 0
  defp matches_summary_window?(summary, :no_window), do: summary.total_windows == 0
  defp matches_summary_window?(_summary, _window), do: true

  defp matches_summary_only_filters?(summary, opts) do
    (not Keyword.get(opts, :only_failed, false) or summary.failure_count > 0) and
      (not Keyword.get(opts, :only_running, false) or summary.active?) and
      (not Keyword.get(opts, :only_incomplete, false) or summary.active?)
  end

  defp summary_activity(summary), do: datetime_sort_value(summary.last_activity_at)

  defp summary_status_priority(summary) do
    cond do
      summary.failure_count > 0 -> 0
      summary.active? -> 1
      true -> 2
    end
  end

  defp datetime_sort_value(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)
  defp datetime_sort_value(_datetime), do: 0

  defp matches_filters?(group, opts) do
    matches_status?(group, Keyword.get(opts, :status)) and
      matches_trigger?(group, Keyword.get(opts, :trigger_type)) and
      matches_target?(group, Keyword.get(opts, :target_asset)) and
      matches_search?(group, Keyword.get(opts, :search)) and
      matches_window?(group, Keyword.get(opts, :window)) and
      matches_only_filters?(group, opts)
  end

  defp matches_status?(_group, nil), do: true
  defp matches_status?(%{root: root}, status), do: root.status == status

  defp matches_trigger?(_group, nil), do: true
  defp matches_trigger?(%{root: root}, trigger), do: RunQuery.trigger_type(root) == trigger

  defp matches_target?(_group, nil), do: true

  defp matches_target?(%{root: root}, target) do
    root
    |> RunQuery.target_refs()
    |> Enum.any?(&(RunQuery.public_ref(&1) == target))
  end

  defp matches_search?(_group, value) when value in [nil, ""], do: true

  defp matches_search?(%{id: id, root: root}, search) do
    search = String.downcase(to_string(search))
    metadata = RunQuery.metadata(root)

    [
      id,
      metadata.trigger_type,
      metadata.asset_ref_text,
      metadata.target_refs_text,
      metadata.window_key
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp matches_window?(_group, nil), do: true
  defp matches_window?(group, :has_window), do: has_window?(group)
  defp matches_window?(group, :no_window), do: not has_window?(group)
  defp matches_window?(_group, _window), do: true

  defp matches_only_filters?(group, opts) do
    (not Keyword.get(opts, :only_failed, false) or failed?(group)) and
      (not Keyword.get(opts, :only_running, false) or running?(group)) and
      (not Keyword.get(opts, :only_incomplete, false) or running?(group))
  end

  defp sort(groups, :failed_first),
    do: Enum.sort_by(groups, &{if(failed?(&1), do: 0, else: 1), -&1.activity})

  defp sort(groups, :running_first),
    do: Enum.sort_by(groups, &{if(running?(&1), do: 0, else: 1), -&1.activity})

  defp sort(groups, :status_priority),
    do: Enum.sort_by(groups, &{status_priority(&1), -&1.activity})

  defp sort(groups, _sort), do: Enum.sort_by(groups, & &1.activity, :desc)

  defp has_window?(%{runs: runs}) do
    Enum.any?(runs, &(RunQuery.metadata(&1).window_key not in [nil, ""]))
  end

  defp failed?(%{root: root, runs: runs}) do
    root.status in [:error, :partial, :cancelled, :timed_out] or
      Enum.any?(runs, &(&1.status in [:error, :partial, :cancelled, :timed_out]))
  end

  defp running?(%{root: root, runs: runs}) do
    root.status in [:pending, :running] or Enum.any?(runs, &(&1.status in [:pending, :running]))
  end

  defp status_priority(group) do
    cond do
      failed?(group) -> 0
      running?(group) -> 1
      true -> 2
    end
  end

  defp page_opts(opts) do
    {:ok, normalized} = Page.normalize_opts(opts)
    normalized
  end
end
