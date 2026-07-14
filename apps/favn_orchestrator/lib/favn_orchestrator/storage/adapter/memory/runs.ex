defmodule FavnOrchestrator.Storage.Adapter.Memory.Runs do
  @moduledoc """
  Pure run snapshot operations for the in-memory adapter.

  The execution-group index avoids rescanning every stored run whenever a run
  snapshot changes or a group is read.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.Storage.WriteSemantics

  @type write_result :: :ok | :idempotent | {:error, term()}

  @doc false
  @spec put(State.t(), RunState.t()) :: {write_result(), State.t()}
  def put(%State{} = state, %RunState{} = incoming) do
    case Map.fetch(state.runs, incoming.id) do
      :error ->
        {:ok, store(state, nil, incoming)}

      {:ok, %RunState{} = existing} ->
        case WriteSemantics.decide(
               existing.event_seq,
               existing.snapshot_hash,
               incoming.event_seq,
               incoming.snapshot_hash
             ) do
          :replace -> {:ok, store(state, existing, incoming)}
          :idempotent -> {:idempotent, state}
          {:error, reason} -> {{:error, reason}, state}
        end
    end
  end

  @doc false
  @spec get(State.t(), String.t()) :: {:ok, RunState.t()} | {:error, :not_found}
  def get(%State{} = state, run_id) do
    case Map.fetch(state.runs, run_id) do
      {:ok, %RunState{} = run} -> {:ok, run}
      :error -> {:error, :not_found}
    end
  end

  @doc false
  @spec list(State.t(), keyword()) :: [RunState.t()]
  def list(%State{} = state, opts) do
    state.runs
    |> Map.values()
    |> filter(opts)
    |> Enum.sort_by(&sort_key/1, :desc)
    |> maybe_limit(opts)
  end

  @doc false
  @spec list_target(State.t(), String.t(), :asset | :pipeline, term(), keyword()) ::
          [RunState.t()]
  def list_target(%State{} = state, manifest_version_id, target_kind, target_ref, opts) do
    state.runs
    |> Map.values()
    |> filter(Keyword.put(opts, :manifest_version_id, manifest_version_id))
    |> Enum.filter(&target?(&1, target_kind, target_ref))
    |> Enum.sort_by(&sort_key/1, :desc)
    |> maybe_limit(opts)
  end

  @doc false
  @spec group(State.t(), String.t()) :: [RunState.t()]
  def group(%State{} = state, group_id) do
    state.execution_group_run_ids
    |> Map.get(group_id, MapSet.new())
    |> Enum.flat_map(fn run_id ->
      case Map.fetch(state.runs, run_id) do
        {:ok, run} -> [run]
        :error -> []
      end
    end)
    |> Enum.sort_by(&group_sort_key/1)
  end

  @doc false
  @spec group_ids(State.t()) :: [String.t()]
  def group_ids(%State{} = state), do: Map.keys(state.execution_group_run_ids)

  @doc false
  @spec sort_key(RunState.t()) :: integer()
  def sort_key(%RunState{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  def sort_key(%RunState{}), do: 0

  defp store(state, existing, incoming) do
    index =
      state.execution_group_run_ids
      |> remove_from_previous_group(existing)
      |> put_in_group(incoming)

    %{
      state
      | runs: Map.put(state.runs, incoming.id, incoming),
        execution_group_run_ids: index
    }
  end

  defp remove_from_previous_group(index, nil), do: index

  defp remove_from_previous_group(index, %RunState{} = run) do
    group_id = RunQuery.root_execution_group_id(run)

    case Map.get(index, group_id) do
      nil ->
        index

      run_ids ->
        remaining = MapSet.delete(run_ids, run.id)

        if MapSet.size(remaining) == 0,
          do: Map.delete(index, group_id),
          else: Map.put(index, group_id, remaining)
    end
  end

  defp put_in_group(index, %RunState{} = run) do
    group_id = RunQuery.root_execution_group_id(run)
    Map.update(index, group_id, MapSet.new([run.id]), &MapSet.put(&1, run.id))
  end

  defp filter(runs, opts) do
    Enum.filter(runs, fn run ->
      matches?(run, :status, Keyword.get(opts, :status)) and
        matches?(run, :manifest_version_id, Keyword.get(opts, :manifest_version_id)) and
        matches_pipeline?(run, Keyword.get(opts, :pipeline_module))
    end)
  end

  defp matches_pipeline?(_run, nil), do: true

  defp matches_pipeline?(%RunState{} = run, expected),
    do: pipeline_submit_ref_text(run) == RunQuery.public_ref(expected)

  defp target?(%RunState{} = run, :asset, target_ref) do
    target_ref_text = RunQuery.public_ref(target_ref)

    run
    |> RunQuery.target_refs()
    |> Enum.any?(&(RunQuery.public_ref(&1) == target_ref_text))
  end

  defp target?(%RunState{} = run, :pipeline, target_ref) do
    pipeline_submit_ref_text(run) == RunQuery.public_ref(target_ref)
  end

  defp pipeline_submit_ref_text(%RunState{} = run) do
    metadata = run.metadata || %{}

    case Map.get(metadata, :pipeline_submit_ref, Map.get(metadata, "pipeline_submit_ref")) do
      value when is_atom(value) or is_binary(value) -> RunQuery.public_ref(value)
      _other -> ""
    end
  end

  defp group_sort_key(%RunState{id: id} = run) do
    case RunQuery.root_execution_group_id(run) do
      ^id -> {0, id}
      _other -> {1, id}
    end
  end

  defp matches?(_run, _field, nil), do: true
  defp matches?(run, field, expected), do: Map.get(run, field) == expected

  defp maybe_limit(runs, opts) do
    case Keyword.get(opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(runs, limit)
      _other -> runs
    end
  end
end
