defmodule FavnOrchestrator.RunServer.Execution.StageAttemptState do
  @moduledoc """
  Explicit state for one pipeline stage attempt.

  This struct owns scheduler bookkeeping for admitted, deferred, retryable, and
  completed same-stage work. Result and retry lists are reverse accumulators so
  concurrent settlement remains constant-time. It does not acquire leases,
  start runner work, or persist run events.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunServer.Execution.ResultBuilder

  @type entry :: map()
  @type node_key :: Favn.Plan.node_key()
  @type terminal_failure :: %{required(:status) => RunState.status(), required(:error) => term()}

  @type t :: %__MODULE__{
          run: RunState.t(),
          results: [term()],
          retry_refs: [node_key()],
          retry_ref_set: MapSet.t(node_key()),
          retry_delays: %{optional(node_key()) => non_neg_integer()},
          terminal_failure: terminal_failure() | nil,
          pending_ids: MapSet.t(String.t()),
          deferred_node_keys: [node_key()],
          queued_steps: MapSet.t(term()),
          attempted_node_keys: [node_key()],
          attempted_node_key_set: MapSet.t(node_key()),
          node_statuses: %{optional(node_key()) => atom()}
        }

  defstruct run: nil,
            results: [],
            retry_refs: [],
            retry_ref_set: MapSet.new(),
            retry_delays: %{},
            terminal_failure: nil,
            pending_ids: MapSet.new(),
            deferred_node_keys: [],
            queued_steps: MapSet.new(),
            attempted_node_keys: [],
            attempted_node_key_set: MapSet.new(),
            node_statuses: %{}

  @spec new(RunState.t(), [term()], [entry()], [node_key()], MapSet.t(term())) :: t()
  def new(%RunState{} = run, results, entries, deferred_node_keys, queued_steps)
      when is_list(results) and is_list(entries) and is_list(deferred_node_keys) do
    node_keys = entry_node_keys(entries)

    %__MODULE__{
      run: run,
      results: results |> Enum.reverse() |> ResultBuilder.retain_asset_results(),
      pending_ids: pending_execution_ids(entries),
      deferred_node_keys: deferred_node_keys,
      queued_steps: queued_steps,
      attempted_node_keys: Enum.reverse(node_keys),
      attempted_node_key_set: MapSet.new(node_keys)
    }
  end

  @spec add_entries(t(), [entry()], RunState.t(), [node_key()], MapSet.t(term())) :: t()
  def add_entries(
        %__MODULE__{} = state,
        entries,
        %RunState{} = run,
        deferred_node_keys,
        queued_steps
      )
      when is_list(entries) and is_list(deferred_node_keys) do
    state = put_attempted_node_keys(state, entry_node_keys(entries))

    %{
      state
      | run: run,
        pending_ids: MapSet.union(state.pending_ids, pending_execution_ids(entries)),
        deferred_node_keys: deferred_node_keys,
        queued_steps: queued_steps
    }
  end

  @spec defer_only(t(), RunState.t(), [node_key()], MapSet.t(term())) :: t()
  def defer_only(%__MODULE__{} = state, %RunState{} = run, deferred_node_keys, queued_steps)
      when is_list(deferred_node_keys) do
    %{state | run: run, deferred_node_keys: deferred_node_keys, queued_steps: queued_steps}
  end

  @spec add_admission_retry(t(), node_key(), non_neg_integer()) :: t()
  def add_admission_retry(%__MODULE__{} = state, node_key, retry_delay_ms)
      when is_integer(retry_delay_ms) and retry_delay_ms >= 0 do
    state
    |> put_retry_node_key(node_key)
    |> put_attempted_node_keys([node_key])
    |> then(&%{&1 | retry_delays: Map.put(&1.retry_delays, node_key, retry_delay_ms)})
  end

  @spec record_result(
          t(),
          RunState.t(),
          [term()],
          [node_key()],
          %{optional(node_key()) => non_neg_integer()},
          terminal_failure() | nil,
          MapSet.t(String.t())
        ) :: t()
  def record_result(
        %__MODULE__{} = state,
        %RunState{} = run,
        results,
        retry_refs,
        retry_delays,
        terminal_failure,
        pending_ids
      )
      when is_list(results) and is_list(retry_refs) and is_map(retry_delays) do
    retry_ref_set =
      case retry_refs do
        [node_key | _rest] -> MapSet.put(state.retry_ref_set, node_key)
        [] -> state.retry_ref_set
      end

    %{
      state
      | run: run,
        results: ResultBuilder.retain_asset_results(results),
        retry_refs: retry_refs,
        retry_ref_set: retry_ref_set,
        retry_delays: retry_delays,
        terminal_failure: terminal_failure,
        pending_ids: pending_ids
    }
  end

  @spec attempted_node_keys(t()) :: [node_key()]
  def attempted_node_keys(%__MODULE__{attempted_node_keys: attempted_node_keys}),
    do: Enum.reverse(attempted_node_keys)

  @doc false
  @spec put_node_status(t(), node_key(), atom()) :: t()
  def put_node_status(%__MODULE__{} = state, node_key, status) when is_atom(status) do
    %{state | node_statuses: Map.put(state.node_statuses, node_key, status)}
  end

  @doc false
  @spec settled_results(t()) :: [term()]
  def settled_results(%__MODULE__{results: results}), do: Enum.reverse(results)

  @doc false
  @spec retry_node_keys(t()) :: [node_key()]
  def retry_node_keys(%__MODULE__{retry_refs: retry_refs}), do: Enum.reverse(retry_refs)

  @doc false
  @spec retry_delays(t()) :: %{optional(node_key()) => non_neg_integer()}
  def retry_delays(%__MODULE__{retry_delays: retry_delays}), do: retry_delays

  defp pending_execution_ids(entries) when is_list(entries) do
    entries
    |> Enum.map(&Map.get(&1, :execution_id))
    |> Enum.filter(&is_binary/1)
    |> MapSet.new()
  end

  defp entry_node_keys(entries) when is_list(entries), do: Enum.map(entries, & &1.node_key)

  defp put_retry_node_key(%__MODULE__{} = state, node_key) do
    if MapSet.member?(state.retry_ref_set, node_key) do
      state
    else
      %{
        state
        | retry_refs: [node_key | state.retry_refs],
          retry_ref_set: MapSet.put(state.retry_ref_set, node_key)
      }
    end
  end

  defp put_attempted_node_keys(%__MODULE__{} = state, node_keys) do
    Enum.reduce(node_keys, state, fn node_key, acc ->
      if MapSet.member?(acc.attempted_node_key_set, node_key) do
        acc
      else
        %{
          acc
          | attempted_node_keys: [node_key | acc.attempted_node_keys],
            attempted_node_key_set: MapSet.put(acc.attempted_node_key_set, node_key)
        }
      end
    end)
  end
end
