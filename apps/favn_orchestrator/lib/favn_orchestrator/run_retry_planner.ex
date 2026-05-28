defmodule FavnOrchestrator.RunRetryPlanner do
  @moduledoc """
  Plans retry submissions for unfinished work in persisted runs.

  The planner is side-effect free. It reads authoritative run snapshots and
  backfill-window rows, then returns the child retry submissions needed to run
  failed or not-started assets while excluding already successful work.
  """

  alias Favn.Plan
  alias Favn.Window.Anchor
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @terminal_retryable_statuses [:error, :partial, :cancelled, :timed_out]
  @successful_statuses [:ok, :skipped_fresh, "ok", "skipped_fresh"]

  @type retry_child :: %{
          required(:source_run_id) => String.t(),
          required(:target_refs) => [Favn.Ref.t()],
          required(:node_keys) => [Plan.node_key()],
          optional(:backfill_run_id) => String.t(),
          optional(:window_key) => String.t(),
          optional(:pipeline_module) => module(),
          optional(:anchor_window) => Anchor.t(),
          optional(:refresh_policy) => map()
        }

  @type retry_plan :: %{
          required(:source_run_id) => String.t(),
          required(:children) => [retry_child()],
          required(:asset_count) => non_neg_integer()
        }

  @doc "Plans remaining work for a failed or partial run/execution group."
  @spec remaining(String.t()) :: {:ok, retry_plan()} | {:error, term()}
  def remaining(run_id) when is_binary(run_id) do
    with {:ok, run} <- Storage.get_run(run_id) do
      remaining_for_run(run)
    end
  end

  defp remaining_for_run(%RunState{submit_kind: kind} = run)
       when kind in [:backfill_asset, :backfill_pipeline] do
    with :ok <- ensure_retryable_run(run),
         {:ok, windows} <- list_retryable_backfill_windows(run.id),
         {:ok, children} <- backfill_children(windows) do
      retry_plan(run.id, children)
    end
  end

  defp remaining_for_run(%RunState{} = run) do
    with :ok <- ensure_retryable_run(run),
         {:ok, child} <- child_retry(run, %{}) do
      retry_plan(run.id, [child])
    end
  end

  defp backfill_children(windows) do
    windows
    |> Enum.reduce_while({:ok, []}, fn window, {:ok, acc} ->
      case window_retry_child(window) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, child} -> {:cont, {:ok, [child | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, children} -> {:ok, Enum.reverse(children)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp window_retry_child(%BackfillWindow{latest_attempt_run_id: run_id} = window)
       when is_binary(run_id) do
    with {:ok, source_run} <- Storage.get_run(run_id),
         {:ok, anchor} <- window_anchor(window),
         {:ok, child} <-
           child_retry(source_run, %{
             backfill_run_id: window.backfill_run_id,
             window_key: window.window_key,
             pipeline_module: window.pipeline_module,
             anchor_window: anchor
           }) do
      {:ok, child}
    end
  end

  defp window_retry_child(%BackfillWindow{}), do: {:ok, nil}

  defp child_retry(%RunState{} = source_run, context) do
    with :ok <- ensure_retryable_run(source_run),
         {:ok, remaining_node_keys} <- remaining_node_keys(source_run) do
      target_refs = target_refs_for_node_keys(source_run.plan, remaining_node_keys)

      {:ok,
       context
       |> Map.merge(%{
         source_run_id: source_run.id,
         target_refs: target_refs,
         node_keys: remaining_node_keys
       })
       |> maybe_put_refresh_policy(source_run)
       |> maybe_put_anchor(source_run)}
    end
  end

  defp retry_plan(_source_run_id, []), do: {:error, :no_remaining_work}

  defp retry_plan(source_run_id, children) do
    {:ok,
     %{
       source_run_id: source_run_id,
       children: children,
       asset_count: children |> Enum.map(&length(&1.target_refs)) |> Enum.sum()
     }}
  end

  defp ensure_retryable_run(%RunState{status: status})
       when status in @terminal_retryable_statuses,
       do: :ok

  defp ensure_retryable_run(%RunState{status: status}), do: {:error, {:run_not_retryable, status}}

  defp remaining_node_keys(%RunState{plan: %Plan{} = plan, result: result}) do
    planned = planned_node_keys(plan)
    node_results = result_entries(result, :node_results)
    successful_node_keys = successful_node_keys(node_results)

    successful_refs =
      if node_results == [] do
        successful_refs(result)
      else
        MapSet.new()
      end

    remaining =
      Enum.reject(planned, fn node_key ->
        MapSet.member?(successful_node_keys, node_key) ||
          MapSet.member?(successful_refs, plan.nodes[node_key].ref)
      end)

    case remaining do
      [] -> {:error, :no_remaining_work}
      [_ | _] -> {:ok, remaining}
    end
  end

  defp remaining_node_keys(%RunState{}), do: {:error, :run_has_no_plan}

  defp planned_node_keys(%Plan{node_stages: stages}) when is_list(stages) and stages != [],
    do: List.flatten(stages)

  defp planned_node_keys(%Plan{nodes: nodes}), do: Map.keys(nodes)

  defp successful_node_keys(node_results) when is_list(node_results) do
    node_results
    |> Enum.filter(&(result_status(&1) in @successful_statuses))
    |> Enum.map(&result_node_key/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  defp successful_node_keys(_node_results), do: MapSet.new()

  defp successful_refs(result) when is_map(result) do
    result
    |> result_entries(:asset_results)
    |> Enum.filter(&(result_status(&1) in @successful_statuses))
    |> Enum.map(&result_ref/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  defp successful_refs(_result), do: MapSet.new()

  defp result_entries(result, field) when is_map(result) do
    result
    |> Map.get(field, Map.get(result, Atom.to_string(field), []))
    |> result_values()
  end

  defp result_entries(_result, _field), do: []

  defp result_values(values) when is_list(values), do: values
  defp result_values(values) when is_map(values), do: Map.values(values)
  defp result_values(_values), do: []

  defp result_status(%{status: status}), do: status
  defp result_status(%{"status" => status}), do: status
  defp result_status(_result), do: nil

  defp result_node_key(%{node_key: node_key}), do: node_key
  defp result_node_key(%{"node_key" => node_key}), do: node_key
  defp result_node_key(_result), do: nil

  defp result_ref(%{ref: ref}), do: ref
  defp result_ref(%{"ref" => ref}), do: ref
  defp result_ref(_result), do: nil

  defp target_refs_for_node_keys(%Plan{} = plan, node_keys) do
    node_keys
    |> Enum.map(&plan.nodes[&1].ref)
    |> Enum.uniq()
  end

  defp maybe_put_anchor(context, %RunState{metadata: metadata}) when is_map(metadata) do
    case Map.get(context, :anchor_window) || metadata_anchor(metadata) do
      nil -> context
      anchor -> Map.put(context, :anchor_window, anchor)
    end
  end

  defp maybe_put_anchor(context, _run), do: context

  defp maybe_put_refresh_policy(context, %RunState{} = run) do
    case refresh_policy(run) do
      nil -> context
      policy -> Map.put(context, :refresh_policy, policy)
    end
  end

  defp refresh_policy(%RunState{metadata: metadata}) when is_map(metadata) do
    Map.get(metadata, :refresh_policy) || Map.get(metadata, "refresh_policy")
  end

  defp refresh_policy(_run), do: nil

  defp metadata_anchor(metadata) do
    metadata
    |> Map.get(:pipeline_context, Map.get(metadata, "pipeline_context", %{}))
    |> case do
      %{anchor_window: %Anchor{} = anchor} -> anchor
      %{"anchor_window" => %Anchor{} = anchor} -> anchor
      _other -> nil
    end
  end

  defp window_anchor(%BackfillWindow{} = window) do
    Anchor.new(window.window_kind, window.window_start_at, window.window_end_at,
      timezone: window.timezone
    )
  end

  defp list_retryable_backfill_windows(backfill_run_id) do
    @terminal_retryable_statuses
    |> Enum.reduce_while({:ok, []}, fn status, {:ok, acc} ->
      case list_backfill_windows_by_status(backfill_run_id, status) do
        {:ok, windows} -> {:cont, {:ok, acc ++ windows}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp list_backfill_windows_by_status(backfill_run_id, status, cursor \\ nil, acc \\ []) do
    case Storage.scan_backfill_windows(
           [backfill_run_id: backfill_run_id, status: status],
           [{:limit, 500}, {:after, cursor}]
         ) do
      {:ok, %{items: items, has_more?: true, next_cursor: next_cursor}}
      when is_map(next_cursor) ->
        list_backfill_windows_by_status(
          backfill_run_id,
          status,
          next_cursor,
          prepend_page_items(items, acc)
        )

      {:ok, %{items: items}} ->
        {:ok, Enum.reverse(prepend_page_items(items, acc))}

      {:error, _reason} = error ->
        error
    end
  end

  defp prepend_page_items(items, acc), do: Enum.reduce(items, acc, &[&1 | &2])
end
