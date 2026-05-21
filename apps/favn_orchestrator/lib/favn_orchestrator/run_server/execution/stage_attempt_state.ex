defmodule FavnOrchestrator.RunServer.Execution.StageAttemptState do
  @moduledoc """
  Explicit state for one pipeline stage attempt.

  This struct owns scheduler bookkeeping for admitted, deferred, retryable, and
  completed same-stage work. It does not acquire leases, start runner work, or
  persist run events.
  """

  alias FavnOrchestrator.RunState

  @type entry :: map()
  @type node_key :: Favn.Plan.node_key()
  @type terminal_failure :: %{required(:status) => RunState.status(), required(:error) => term()}

  @type t :: %__MODULE__{
          run: RunState.t(),
          results: [term()],
          retry_refs: [node_key()],
          terminal_failure: terminal_failure() | nil,
          pending_ids: MapSet.t(String.t()),
          deferred_node_keys: [node_key()],
          queued_steps: MapSet.t(term()),
          initial_entries: [entry()],
          attempted_node_keys: [node_key()]
        }

  defstruct run: nil,
            results: [],
            retry_refs: [],
            terminal_failure: nil,
            pending_ids: MapSet.new(),
            deferred_node_keys: [],
            queued_steps: MapSet.new(),
            initial_entries: [],
            attempted_node_keys: []

  @spec new(RunState.t(), [term()], [entry()], [node_key()], MapSet.t(term())) :: t()
  def new(%RunState{} = run, results, entries, deferred_node_keys, queued_steps)
      when is_list(results) and is_list(entries) and is_list(deferred_node_keys) do
    %__MODULE__{
      run: run,
      results: results,
      pending_ids: pending_execution_ids(entries),
      deferred_node_keys: deferred_node_keys,
      queued_steps: queued_steps,
      initial_entries: entries,
      attempted_node_keys: entry_node_keys(entries)
    }
  end

  @spec terminal_failure?(t()) :: boolean()
  def terminal_failure?(%__MODULE__{terminal_failure: nil}), do: false
  def terminal_failure?(%__MODULE__{}), do: true

  @spec deferred?(t()) :: boolean()
  def deferred?(%__MODULE__{deferred_node_keys: []}), do: false
  def deferred?(%__MODULE__{}), do: true

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

  @spec record_result(
          t(),
          RunState.t(),
          [term()],
          [node_key()],
          terminal_failure() | nil,
          MapSet.t(String.t())
        ) :: t()
  def record_result(
        %__MODULE__{} = state,
        %RunState{} = run,
        results,
        retry_refs,
        terminal_failure,
        pending_ids
      )
      when is_list(results) and is_list(retry_refs) do
    %{
      state
      | run: run,
        results: results,
        retry_refs: retry_refs,
        terminal_failure: terminal_failure,
        pending_ids: pending_ids
    }
  end

  @spec mark_terminal_failure(t(), terminal_failure()) :: t()
  def mark_terminal_failure(%__MODULE__{} = state, terminal_failure)
      when is_map(terminal_failure),
      do: %{state | terminal_failure: terminal_failure}

  @spec attempted_node_keys(t()) :: [node_key()]
  def attempted_node_keys(%__MODULE__{attempted_node_keys: attempted_node_keys}),
    do: attempted_node_keys

  @spec pending_execution_ids([entry()]) :: MapSet.t(String.t())
  def pending_execution_ids(entries) when is_list(entries) do
    entries
    |> Enum.map(&Map.get(&1, :execution_id))
    |> Enum.filter(&is_binary/1)
    |> MapSet.new()
  end

  @spec entry_node_keys([entry()]) :: [node_key()]
  def entry_node_keys(entries) when is_list(entries), do: Enum.map(entries, & &1.node_key)
end
