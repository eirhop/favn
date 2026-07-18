defmodule FavnOrchestrator.RunRetryPlanner do
  @moduledoc """
  Plans retry submissions for unfinished work in one persisted run.

  The planner is side-effect free and workspace-scoped. Backfill roots are not
  expanded here: the durable backfill ledger owns bounded claiming and retry of
  backfill windows.
  """

  alias Favn.Plan
  alias Favn.Window.Anchor
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs

  @terminal_retryable_statuses [:error, :partial, :cancelled, :timed_out]
  @successful_statuses [:ok, :skipped_fresh, "ok", "skipped_fresh"]

  @type retry_child :: %{
          required(:source_run_id) => String.t(),
          required(:target_refs) => [Favn.Ref.t()],
          required(:node_keys) => [Plan.node_key()],
          optional(:anchor_window) => Anchor.t(),
          optional(:refresh_policy) => map()
        }

  @type retry_plan :: %{
          required(:source_run_id) => String.t(),
          required(:children) => [retry_child()],
          required(:asset_count) => non_neg_integer()
        }

  @doc "Plans remaining work for one failed or partial run in a workspace."
  @spec remaining(WorkspaceContext.t(), String.t()) ::
          {:ok, retry_plan()} | {:error, term()}
  def remaining(%WorkspaceContext{} = context, run_id) when is_binary(run_id) do
    with {:ok, run} <- Runs.get(context, run_id) do
      remaining_for_run(run)
    end
  end

  defp remaining_for_run(%RunState{submit_kind: kind} = run)
       when kind in [:backfill_asset, :backfill_pipeline] do
    with :ok <- ensure_retryable_run(run) do
      {:error, :backfill_retry_managed_by_ledger}
    end
  end

  defp remaining_for_run(%RunState{} = run) do
    with :ok <- ensure_retryable_run(run),
         {:ok, child} <- child_retry(run) do
      {:ok,
       %{
         source_run_id: run.id,
         children: [child],
         asset_count: length(child.target_refs)
       }}
    end
  end

  defp child_retry(%RunState{} = source_run) do
    with {:ok, remaining_node_keys} <- remaining_node_keys(source_run),
         {:ok, target_refs} <- target_refs_for_node_keys(source_run.plan, remaining_node_keys) do
      {:ok,
       %{
         source_run_id: source_run.id,
         target_refs: target_refs,
         node_keys: remaining_node_keys
       }
       |> maybe_put_refresh_policy(source_run)
       |> maybe_put_anchor(source_run)}
    end
  end

  defp ensure_retryable_run(%RunState{status: status} = run)
       when status in @terminal_retryable_statuses do
    if truncated_result?(run) do
      {:error, :run_result_truncated}
    else
      :ok
    end
  end

  defp ensure_retryable_run(%RunState{status: status}),
    do: {:error, {:run_not_retryable, status}}

  defp truncated_result?(%RunState{} = run) do
    run.result
    |> result_metadata()
    |> field(:result_retention, %{})
    |> field(:truncated, false)
  end

  defp result_metadata(result) when is_map(result), do: field(result, :metadata, %{})
  defp result_metadata(_result), do: %{}

  defp field(value, key, default) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))

  defp remaining_node_keys(%RunState{plan: %Plan{} = plan, result: result}) do
    planned = planned_node_keys(plan)
    node_results = result_entries(result, :node_results)
    successful_node_keys = successful_node_keys(node_results)

    successful_refs =
      if node_results == [], do: successful_refs(result), else: MapSet.new()

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
    Enum.reduce_while(node_keys, {:ok, []}, fn node_key, {:ok, acc} ->
      case Map.fetch(plan.nodes, node_key) do
        {:ok, %{ref: ref}} -> {:cont, {:ok, [ref | acc]}}
        _missing -> {:halt, {:error, {:invalid_retry_plan_node, node_key}}}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, refs |> Enum.reverse() |> Enum.uniq()}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_put_anchor(context, %RunState{metadata: metadata}) when is_map(metadata) do
    case metadata_anchor(metadata) do
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
end
