defmodule FavnOrchestrator.TargetAdmission do
  @moduledoc """
  Preflights ordinary run plans against persisted target compatibility.

  Admission considers only assets present in the concrete selected plan. A
  blocked upstream therefore rejects the dependent path without preventing an
  unrelated plan from running.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Plan
  alias Favn.TargetIdentity
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.Results.TargetBinding
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @binding_batch 500
  @max_reported_path_targets 64
  @writable_statuses [:ready, :uninitialized, :rebuild_available]
  @blocked_statuses %{
    rebuild_required: :rebuild_required,
    unexpected_drift: :target_drift,
    operator_decision: :operator_decision_required
  }

  @type error_code :: :rebuild_required | :target_drift | :operator_decision_required
  @type blocked_details :: %{
          required(:target_id) => String.t(),
          required(:selected_target_id) => String.t(),
          required(:blocked_path) => [String.t()],
          required(:blocked_path_target_count) => pos_integer(),
          required(:blocked_path_truncated) => boolean(),
          required(:compatibility_status) => TargetBinding.compatibility_status(),
          required(:reason_code) => String.t()
        }

  @doc "Checks the selected plan's persisted targets before generation pinning mutates state."
  @spec preflight(WorkspaceContext.t(), Index.t(), Plan.t()) ::
          :ok | {:error, {error_code(), blocked_details()}} | {:error, term()}
  def preflight(%WorkspaceContext{} = context, %Index{} = index, %Plan{} = plan) do
    with {:ok, target_ids} <- selected_persisted_target_ids(index, plan),
         {:ok, bindings} <- fetch_bindings(context, target_ids),
         :ok <- ensure_complete_bindings(plan, target_ids, bindings) do
      check(plan, bindings)
    end
  end

  @doc "Pure compatibility check for an already-fetched binding set."
  @spec check(Plan.t(), [TargetBinding.t()]) ::
          :ok | {:error, {error_code(), blocked_details()}}
  def check(%Plan{} = plan, bindings) when is_list(bindings) do
    selected_target_ids = plan |> selected_target_ids() |> MapSet.new()

    blocked_binding =
      bindings
      |> Enum.filter(&MapSet.member?(selected_target_ids, &1.target_id))
      |> Enum.reject(&(&1.compatibility_status in @writable_statuses))
      |> Enum.sort_by(& &1.target_id)
      |> List.first()

    case blocked_binding do
      nil ->
        :ok

      %TargetBinding{} = binding ->
        error_code = Map.fetch!(@blocked_statuses, binding.compatibility_status)
        path = blocked_path(plan, binding.target_id)
        {reported_path, truncated?} = bounded_path(path)

        {:error,
         {error_code,
          %{
            target_id: binding.target_id,
            selected_target_id: List.last(path),
            blocked_path: reported_path,
            blocked_path_target_count: length(path),
            blocked_path_truncated: truncated?,
            compatibility_status: binding.compatibility_status,
            reason_code: binding.reason_code
          }}}
    end
  end

  defp selected_persisted_target_ids(%Index{} = index, %Plan{} = plan) do
    plan
    |> selected_refs()
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, target_ids} ->
      case Index.fetch_asset(index, ref) do
        {:ok, %Asset{target_descriptor: %TargetDescriptor{}}} ->
          {:cont, {:ok, [TargetIdentity.for_asset(ref) | target_ids]}}

        {:ok, %Asset{}} ->
          {:cont, {:ok, target_ids}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, target_ids} -> {:ok, target_ids |> Enum.uniq() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_bindings(_context, []), do: {:ok, []}

  defp fetch_bindings(%WorkspaceContext{} = context, target_ids) do
    target_ids
    |> Enum.chunk_every(@binding_batch)
    |> Enum.reduce_while({:ok, []}, fn batch, {:ok, bindings} ->
      query = %GetTargetBindings{workspace_context: context, target_ids: batch}

      case Persistence.stores().target_generations.get_bindings(query) do
        {:ok, next_bindings} -> {:cont, {:ok, next_bindings ++ bindings}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ensure_complete_bindings(plan, target_ids, bindings) do
    bound_ids = MapSet.new(bindings, & &1.target_id)

    case Enum.find(target_ids, &(not MapSet.member?(bound_ids, &1))) do
      nil ->
        :ok

      target_id ->
        path = blocked_path(plan, target_id)
        {reported_path, truncated?} = bounded_path(path)

        {:error,
         {:operator_decision_required,
          %{
            target_id: target_id,
            selected_target_id: List.last(path),
            blocked_path: reported_path,
            blocked_path_target_count: length(path),
            blocked_path_truncated: truncated?,
            compatibility_status: :operator_decision,
            reason_code: "target_binding_missing"
          }}}
    end
  end

  defp blocked_path(%Plan{} = plan, blocked_target_id) do
    target_ids = plan.target_refs |> Enum.map(&TargetIdentity.for_asset/1) |> MapSet.new()

    if MapSet.member?(target_ids, blocked_target_id) do
      [blocked_target_id]
    else
      adjacency = target_adjacency(plan)
      find_target_path(blocked_target_id, target_ids, adjacency)
    end
  end

  defp find_target_path(start, target_ids, adjacency) do
    queue = :queue.in({start, [start]}, :queue.new())
    do_find_target_path(queue, MapSet.new([start]), target_ids, adjacency) || [start]
  end

  defp do_find_target_path(queue, visited, target_ids, adjacency) do
    case :queue.out(queue) do
      {:empty, _queue} ->
        nil

      {{:value, {current, path}}, queue} ->
        neighbors = Map.get(adjacency, current, [])

        case Enum.find(neighbors, &MapSet.member?(target_ids, &1)) do
          nil ->
            {queue, visited} =
              Enum.reduce(neighbors, {queue, visited}, fn neighbor, {next_queue, seen} ->
                if MapSet.member?(seen, neighbor) do
                  {next_queue, seen}
                else
                  {
                    :queue.in({neighbor, path ++ [neighbor]}, next_queue),
                    MapSet.put(seen, neighbor)
                  }
                end
              end)

            do_find_target_path(queue, visited, target_ids, adjacency)

          target_id ->
            path ++ [target_id]
        end
    end
  end

  defp target_adjacency(%Plan{} = plan) do
    Enum.reduce(plan.nodes, %{}, fn {_node_key, node}, adjacency ->
      source_id = TargetIdentity.for_asset(node.ref)

      downstream_ids =
        node
        |> Map.get(:downstream, [])
        |> Enum.map(fn downstream_key ->
          downstream = Map.fetch!(plan.nodes, downstream_key)
          TargetIdentity.for_asset(downstream.ref)
        end)
        |> Enum.reject(&(&1 == source_id))

      Map.update(adjacency, source_id, downstream_ids, fn current ->
        downstream_ids ++ current
      end)
    end)
    |> Map.new(fn {target_id, downstream_ids} ->
      {target_id, downstream_ids |> Enum.uniq() |> Enum.sort()}
    end)
  end

  defp selected_target_ids(%Plan{} = plan) do
    plan
    |> selected_refs()
    |> Enum.map(&TargetIdentity.for_asset/1)
  end

  defp selected_refs(%Plan{} = plan) do
    plan.nodes
    |> Map.values()
    |> Enum.map(& &1.ref)
    |> Enum.uniq()
    |> Enum.sort_by(&TargetIdentity.for_asset/1)
  end

  defp bounded_path(path) when length(path) <= @max_reported_path_targets,
    do: {path, false}

  defp bounded_path(path) do
    reported = Enum.take(path, @max_reported_path_targets - 1) ++ [List.last(path)]
    {reported, true}
  end
end
