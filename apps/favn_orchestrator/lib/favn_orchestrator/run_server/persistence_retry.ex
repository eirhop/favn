defmodule FavnOrchestrator.RunServer.PersistenceRetry do
  @moduledoc false

  alias FavnOrchestrator.Persistence, as: Stores
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunServer.FailureCleanup
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec

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

  @spec persist(t()) :: :ok | {:ok, term()} | {:terminal, RunState.t()} | {:error, term()}
  def persist(%__MODULE__{resume: :terminal} = retry) do
    context = SystemContext.workspace(retry.run.workspace_id, :run_worker)

    with {:ok, latest} <- Runs.get(context, retry.run.id) do
      if RunState.finalized?(latest),
        do: {:terminal, latest},
        else: Persistence.persist_run_step(retry.run, retry.event_type, retry.data)
    end
  end

  def persist(%__MODULE__{command: nil} = retry),
    do: Persistence.persist_run_step(retry.run, retry.event_type, retry.data)

  def persist(%__MODULE__{event_type: :runner_admission, command: command, run: run}) do
    permitted =
      with :ok <- FavnOrchestrator.RunLeaseKeeper.permit(run),
           do: FavnOrchestrator.RunTargetMaintenance.register(run, command.enqueue.task_id)

    command =
      case permitted do
        {:ok, observer} -> %{command | acquisition_observer: observer}
        _ -> %{command | reconcile_only?: true}
      end

    result = RunnerTasks.admit(command)
    FavnOrchestrator.RunTargetMaintenance.admission_result(run, command.enqueue.task_id, result)
    Persistence.normalize_result(run, result)
  end

  def persist(%__MODULE__{event_type: :resource_outcomes, command: command}),
    do: ResourceCircuits.persist_settlement(command)

  def persist(%__MODULE__{event_type: :resource_recovery_candidate, command: command}),
    do: Stores.stores().resource_circuits.record_recovery_candidate(command)

  @doc "Resolves an exhausted or rejected transition without repeating its write."
  @spec resolve(t()) ::
          {:committed, RunState.t()}
          | {:terminal, RunState.t()}
          | {:failed, RunState.t()}
          | {:error, term()}
  def resolve(retry) do
    run = retry.run
    context = SystemContext.workspace(run.workspace_id, :run_worker)

    with :ok <- FavnOrchestrator.RunLeaseKeeper.permit(run),
         {:ok, latest} <- Runs.get(context, run.id) do
      cond do
        RunState.finalized?(latest) ->
          {:terminal, latest}

        latest.metadata[:cancel_requested] == true or latest.metadata["cancel_requested"] == true ->
          {:error, :cancellation_race}

        true ->
          with {:ok, page} <-
                 Runs.page_events(context, run.id, after_sequence: run.event_seq - 1, limit: 1) do
            expected = run |> RunState.with_snapshot_hash() |> RunState.for_step_persistence()
            event = Projector.run_event(expected, retry.event_type, retry.data)

            if latest.event_seq == expected.event_seq and
                 same_snapshot?(latest, expected) and
                 Enum.any?(page.items, &same_event?(&1, event)) do
              {:committed, run}
            else
              case FailureCleanup.fail(run, retry.original_error || exhaustion(retry)) do
                {:ok, failed} -> {:failed, failed}
                error -> error
              end
            end
          end
      end
    end
  end

  @doc "Rebases a completed outcome only after observing durable cancellation intent."
  @spec after_cancellation(t()) ::
          {:retry_command, t()} | {:terminal, RunState.t()} | {:error, term()}
  def after_cancellation(retry) do
    context = SystemContext.workspace(retry.run.workspace_id, :run_worker)

    with {:ok, latest} <- Runs.get(context, retry.run.id) do
      if RunState.finalized?(latest) do
        {:terminal, latest}
      else
        run =
          latest
          |> RunState.with_storage_fence(
            retry.run.storage_owner_id,
            retry.run.storage_fencing_token
          )
          |> RunState.transition(
            status: retry.run.status,
            error: retry.run.error,
            result: retry.run.result,
            metadata: Map.merge(retry.run.metadata, latest.metadata)
          )
          |> Map.put(:updated_at, retry.run.updated_at)
          |> RunState.with_snapshot_hash()

        {:retry_command, %{retry | run: run}}
      end
    end
  end

  defp same_snapshot?(saved, expected) do
    with {:ok, left} <- RunSnapshotCodec.encode_run(saved, plan: :reference),
         {:ok, right} <- RunSnapshotCodec.encode_run(expected, plan: :reference) do
      # The in-memory term hash is recomputed after decoding; compare persisted data.
      Map.delete(Jason.decode!(left), "snapshot_hash") ==
        Map.delete(Jason.decode!(right), "snapshot_hash")
    else
      _ -> false
    end
  end

  defp same_event?(saved, expected) do
    with {:ok, encoded} <- RunEventCodec.encode(expected),
         {:ok, decoded} <- RunEventCodec.decode(encoded) do
      Map.put(saved, :global_sequence, nil) == decoded
    else
      _ -> false
    end
  end

  @spec replayable?(term()) :: boolean()
  def replayable?(%Error{kind: kind, retryable?: retryable?})
      when kind in [:unavailable, :timeout, :conflict], do: retryable?

  def replayable?(_reason), do: false

  @doc "Classifies uncertain run transitions, whose original sequence can be reconciled."
  @spec transition_retryable?(term()) :: boolean()
  def transition_retryable?(%Error{kind: kind}) when kind in [:unavailable, :timeout], do: true
  def transition_retryable?(reason), do: replayable?(reason)

  @doc false
  @spec recovery_required?(term()) :: boolean()
  def recovery_required?({:cleanup_read_requires_reconciliation, _task}), do: true

  def recovery_required?(%Error{kind: kind}) when kind in [:unavailable, :timeout], do: true
  def recovery_required?(%Error{kind: :conflict, retryable?: true}), do: true
  def recovery_required?(:runner_task_timeout), do: true

  def recovery_required?({kind, _})
      when kind in [
             :runner_task_waiter_unavailable,
             :runner_task_waiter_stopped,
             :runner_task_data_unavailable
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
