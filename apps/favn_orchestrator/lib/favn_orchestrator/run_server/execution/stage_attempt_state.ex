defmodule FavnOrchestrator.RunServer.Execution.StageAttemptState do
  @moduledoc """
  Explicit state for one pipeline stage attempt.

  This struct owns scheduler bookkeeping for admitted, deferred, retryable, and
  completed same-stage work. Result and retry lists are reverse accumulators so
  concurrent settlement remains constant-time. It does not acquire leases,
  start runner work, or persist run events.
  """

  alias FavnOrchestrator.RunState

  @type entry :: map()
  @type node_key :: Favn.Plan.node_key()
  @type terminal_failure :: %{required(:status) => RunState.status(), required(:error) => term()}

  @type t :: %__MODULE__{
          run: RunState.t(),
          results: [term()],
          retry_refs: [node_key()],
          retry_delays: %{optional(node_key()) => non_neg_integer()},
          terminal_failure: terminal_failure() | nil,
          pending_ids: MapSet.t(String.t()),
          deferred_node_keys: [node_key()],
          queued_steps: MapSet.t(term()),
          attempted_node_keys: [node_key()]
        }

  defstruct run: nil,
            results: [],
            retry_refs: [],
            retry_delays: %{},
            terminal_failure: nil,
            pending_ids: MapSet.new(),
            deferred_node_keys: [],
            queued_steps: MapSet.new(),
            attempted_node_keys: []

  @spec new(RunState.t(), [term()], [entry()], [node_key()], MapSet.t(term())) :: t()
  def new(%RunState{} = run, results, entries, deferred_node_keys, queued_steps)
      when is_list(results) and is_list(entries) and is_list(deferred_node_keys) do
    %__MODULE__{
      run: run,
      results: Enum.reverse(results),
      pending_ids: pending_execution_ids(entries),
      deferred_node_keys: deferred_node_keys,
      queued_steps: queued_steps,
      attempted_node_keys: entry_node_keys(entries)
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
    %{
      state
      | run: run,
        pending_ids: MapSet.union(state.pending_ids, pending_execution_ids(entries)),
        deferred_node_keys: deferred_node_keys,
        queued_steps: queued_steps,
        attempted_node_keys: Enum.uniq(state.attempted_node_keys ++ entry_node_keys(entries))
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
    %{
      state
      | retry_refs: [node_key | Enum.reject(state.retry_refs, &(&1 == node_key))],
        retry_delays: Map.put(state.retry_delays, node_key, retry_delay_ms),
        attempted_node_keys: Enum.uniq(state.attempted_node_keys ++ [node_key])
    }
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
    %{
      state
      | run: run,
        results: results,
        retry_refs: retry_refs,
        retry_delays: retry_delays,
        terminal_failure: terminal_failure,
        pending_ids: pending_ids
    }
  end

  @spec attempted_node_keys(t()) :: [node_key()]
  def attempted_node_keys(%__MODULE__{attempted_node_keys: attempted_node_keys}),
    do: attempted_node_keys

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
end
