defmodule FavnOrchestrator.RunServer.PersistenceRetry do
  @moduledoc false

  alias FavnOrchestrator.Persistence, as: Stores
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunState

  @retry_budget_ms 30_000

  @enforce_keys [:run, :event_type, :data, :resume]
  defstruct @enforce_keys ++
              [
                command: nil,
                result: nil,
                started_ms: nil,
                attempts: 0,
                original_error: nil,
                ambiguous?: false
              ]

  @type operation ::
          :runner_admission
          | :resource_outcomes
          | :resource_recovery_candidate
  @type t :: %__MODULE__{
          run: RunState.t(),
          event_type: atom(),
          data: map(),
          resume: term(),
          command: struct() | nil,
          result: term(),
          started_ms: integer() | nil,
          attempts: non_neg_integer(),
          original_error: term(),
          ambiguous?: boolean()
        }

  @spec new(RunState.t(), atom(), map(), term()) :: t()
  def new(%RunState{} = run, event_type, data, resume)
      when is_atom(event_type) and is_map(data),
      do: %__MODULE__{run: run, event_type: event_type, data: data, resume: resume}

  @spec command(RunState.t(), operation(), struct(), map(), term()) :: t()
  def command(run, operation, command, data, resume),
    do: %{new(run, operation, data, resume) | command: command}

  @spec persist(t()) :: :ok | {:ok, term()} | {:error, term()}
  def persist(%__MODULE__{command: nil} = retry),
    do: Persistence.persist_run_step(retry.run, retry.event_type, retry.data)

  def persist(%__MODULE__{event_type: :runner_admission, command: command, run: run}),
    do: Persistence.normalize_result(run, RunnerTasks.admit(command))

  def persist(%__MODULE__{event_type: :resource_outcomes, command: command}),
    do: ResourceCircuits.persist_settlement(command)

  def persist(%__MODULE__{event_type: :resource_recovery_candidate, command: command}),
    do: Stores.stores().resource_circuits.record_recovery_candidate(command)

  @spec replayable?(term()) :: boolean()
  def replayable?(%Error{kind: kind, retryable?: retryable?})
      when kind in [:unavailable, :timeout, :conflict], do: retryable?

  def replayable?(_reason), do: false

  @doc false
  @spec recovery_required?(term()) :: boolean()
  def recovery_required?(%Error{kind: kind}) when kind in [:unavailable, :timeout], do: true
  def recovery_required?(%Error{kind: :conflict, retryable?: true}), do: true
  def recovery_required?(:runner_task_timeout), do: true

  def recovery_required?({kind, _})
      when kind in [
             :runner_task_waiter_unavailable,
             :runner_task_waiter_stopped,
             :runner_task_data_unavailable,
             :post_step_worker_down
           ],
      do: true

  def recovery_required?({_operation, reason}), do: recovery_required?(reason)
  def recovery_required?(_reason), do: false

  @spec rejected(t(), term()) :: t()
  def rejected(retry, reason),
    do: %{
      retry
      | started_ms: retry.started_ms || System.monotonic_time(:millisecond),
        attempts: retry.attempts + 1,
        original_error: retry.original_error || reason,
        ambiguous?:
          retry.ambiguous? or
            match?(%Error{kind: kind} when kind in [:timeout, :unavailable], reason)
    }

  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{started_ms: nil}), do: false

  def exhausted?(retry),
    do: System.monotonic_time(:millisecond) - retry.started_ms >= @retry_budget_ms

  @spec diagnostics(t()) :: map()
  def diagnostics(retry) do
    phase =
      case retry.resume do
        {:stage_operation, pause} -> pause.phase
        {:sequential_operation, pause} -> pause.phase
        _ -> retry.event_type
      end

    %{
      operation: retry.event_type,
      phase: phase,
      asset_step_id: retry.data[:asset_step_id],
      asset_ref: retry.data[:asset_ref],
      node_key: retry.data[:node_key],
      stage: retry.data[:stage],
      original_error: retry.original_error,
      attempts: retry.attempts,
      retry_budget_ms: @retry_budget_ms,
      elapsed_ms:
        if(retry.started_ms, do: System.monotonic_time(:millisecond) - retry.started_ms, else: 0)
    }
  end

  @spec exhaustion(t()) :: Error.t()
  def exhaustion(retry) do
    Error.new(:timeout, "Control-plane persistence retry budget exhausted",
      details: Map.put(diagnostics(retry), :reason_code, "persistence_retry_exhausted")
    )
  end
end
