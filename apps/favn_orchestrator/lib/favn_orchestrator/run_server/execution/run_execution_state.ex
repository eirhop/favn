defmodule FavnOrchestrator.RunServer.Execution.RunExecutionState do
  @moduledoc """
  Explicit run-server execution state.

  The run server owns this struct across GenServer callbacks. It contains the
  process-owned runtime facts needed to continue work from messages instead of a
  blocking execution call stack.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunServer.Execution.AwaitTasks
  alias FavnOrchestrator.RunServer.Execution.RunWorkSet
  alias FavnOrchestrator.RunServer.Execution.StageAttemptState
  alias FavnOrchestrator.RunState

  @type mode :: :sequential | :pipeline
  @type status ::
          :starting
          | :submitting
          | :awaiting
          | :retry_wait
          | :admission_wait
          | :draining
          | :terminalizing
          | :terminal

  @type await :: %{
          required(:pid) => pid(),
          required(:monitor_ref) => reference(),
          required(:timeout_token) => reference(),
          required(:timeout_ref) => reference(),
          required(:entry) => map(),
          required(:kind) => :sequential | :pipeline
        }

  @type timer_entry :: %{
          required(:timer_ref) => reference(),
          required(:payload) => map()
        }

  @type t :: %__MODULE__{
          run: RunState.t(),
          version: Version.t(),
          mode: mode(),
          status: status(),
          runner_client: module() | nil,
          runner_opts: keyword(),
          work_set: RunWorkSet.t(),
          active_attempts: map(),
          await_tasks: AwaitTasks.t(),
          awaits: %{optional(String.t()) => await()},
          await_monitors: %{optional(reference()) => String.t()},
          await_timers: %{optional(reference()) => String.t()},
          retry_timers: %{optional(reference()) => timer_entry()},
          admission_timers: %{optional(reference()) => timer_entry()},
          accumulated_results: [term()],
          sequential_refs: [{Favn.Ref.t(), Favn.Plan.node_key(), non_neg_integer()}],
          sequential_index: non_neg_integer(),
          stage_groups: [{non_neg_integer(), [Favn.Plan.node_key()]}],
          stage_index: non_neg_integer(),
          stage_state: StageAttemptState.t() | nil,
          stage_attempt: pos_integer(),
          stage_admission_deadline_ms: integer() | nil,
          stage_decisions: map(),
          stage_freshness_context: map() | nil,
          stage_executed_node_keys: [Favn.Plan.node_key()],
          freshness_context: map() | nil,
          terminal_failure: map() | nil,
          terminal?: boolean()
        }

  defstruct run: nil,
            version: nil,
            mode: :sequential,
            status: :starting,
            runner_client: nil,
            runner_opts: [],
            work_set: nil,
            active_attempts: %{},
            await_tasks: AwaitTasks.new(),
            awaits: %{},
            await_monitors: %{},
            await_timers: %{},
            retry_timers: %{},
            admission_timers: %{},
            accumulated_results: [],
            sequential_refs: [],
            sequential_index: 0,
            stage_groups: [],
            stage_index: 0,
            stage_state: nil,
            stage_attempt: 1,
            stage_admission_deadline_ms: nil,
            stage_decisions: %{},
            stage_freshness_context: nil,
            stage_executed_node_keys: [],
            freshness_context: nil,
            terminal_failure: nil,
            terminal?: false

  @doc "Creates base execution state for a run."
  @spec new(RunState.t(), Version.t(), keyword()) :: t()
  def new(%RunState{} = run, %Version{} = version, opts) when is_list(opts) do
    %__MODULE__{
      run: run,
      version: version,
      mode: Keyword.fetch!(opts, :mode),
      runner_client: Keyword.get(opts, :runner_client),
      runner_opts: Keyword.get(opts, :runner_opts, []),
      work_set: RunWorkSet.new(run),
      sequential_refs: Keyword.get(opts, :sequential_refs, []),
      stage_groups: Keyword.get(opts, :stage_groups, []),
      freshness_context: Keyword.get(opts, :freshness_context)
    }
  end

  @doc "Stores active runner work and syncs run in-flight metadata."
  @spec add_work(t(), map()) :: t()
  def add_work(%__MODULE__{} = state, entry) when is_map(entry) do
    work_set = RunWorkSet.add_entry(state.work_set, entry)
    %{state | work_set: work_set, run: RunWorkSet.sync_run_metadata(state.run, work_set)}
  end

  @doc "Removes completed runner work and syncs run in-flight metadata."
  @spec complete_work(t(), String.t()) :: {map() | nil, t()}
  def complete_work(%__MODULE__{} = state, execution_id) when is_binary(execution_id) do
    {entry, work_set} = RunWorkSet.complete_entry(state.work_set, execution_id)
    {entry, %{state | work_set: work_set, run: RunWorkSet.sync_run_metadata(state.run, work_set)}}
  end

  @doc "Stores metadata for an await worker."
  @spec put_await(t(), String.t(), await()) :: t()
  def put_await(%__MODULE__{} = state, execution_id, await) when is_binary(execution_id) do
    %{
      state
      | awaits: Map.put(state.awaits, execution_id, await),
        await_monitors: Map.put(state.await_monitors, await.monitor_ref, execution_id),
        await_timers: Map.put(state.await_timers, await.timeout_token, execution_id)
    }
  end

  @doc "Removes await metadata by execution id."
  @spec pop_await(t(), String.t()) :: {await() | nil, t()}
  def pop_await(%__MODULE__{} = state, execution_id) when is_binary(execution_id) do
    {await, awaits} = Map.pop(state.awaits, execution_id)

    state =
      if await do
        %{
          state
          | awaits: awaits,
            await_monitors: Map.delete(state.await_monitors, await.monitor_ref),
            await_timers: Map.delete(state.await_timers, await.timeout_token)
        }
      else
        %{state | awaits: awaits}
      end

    {await, state}
  end

  @doc "Stores a retry timer."
  @spec put_retry_timer(t(), reference(), reference(), map()) :: t()
  def put_retry_timer(%__MODULE__{} = state, timer_token, timer_ref, retry) do
    entry = %{timer_ref: timer_ref, payload: retry}
    %{state | retry_timers: Map.put(state.retry_timers, timer_token, entry), status: :retry_wait}
  end

  @doc "Removes a retry timer."
  @spec pop_retry_timer(t(), reference()) :: {timer_entry() | nil, t()}
  def pop_retry_timer(%__MODULE__{} = state, timer_token) do
    {retry, timers} = Map.pop(state.retry_timers, timer_token)
    {retry, %{state | retry_timers: timers}}
  end

  @doc "Stores an admission retry timer."
  @spec put_admission_timer(t(), reference(), reference(), map()) :: t()
  def put_admission_timer(%__MODULE__{} = state, timer_token, timer_ref, retry) do
    entry = %{timer_ref: timer_ref, payload: retry}

    %{
      state
      | admission_timers: Map.put(state.admission_timers, timer_token, entry),
        status: :admission_wait
    }
  end

  @doc "Removes an admission retry timer."
  @spec pop_admission_timer(t(), reference()) :: {timer_entry() | nil, t()}
  def pop_admission_timer(%__MODULE__{} = state, timer_token) do
    {retry, timers} = Map.pop(state.admission_timers, timer_token)
    {retry, %{state | admission_timers: timers}}
  end

  @doc "Cancels all owned timers and clears timer indexes."
  @spec cancel_timers(t()) :: t()
  def cancel_timers(%__MODULE__{} = state) do
    state.awaits
    |> Map.values()
    |> Enum.each(&Process.cancel_timer(&1.timeout_ref))

    state.retry_timers
    |> Map.values()
    |> Enum.each(&Process.cancel_timer(&1.timer_ref))

    state.admission_timers
    |> Map.values()
    |> Enum.each(&Process.cancel_timer(&1.timer_ref))

    %{state | await_timers: %{}, retry_timers: %{}, admission_timers: %{}}
  end
end
