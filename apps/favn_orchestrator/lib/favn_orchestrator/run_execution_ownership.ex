defmodule FavnOrchestrator.RunExecutionOwnership do
  @moduledoc """
  Orchestrator-owned runner execution ownership records.

  Ownership is persisted with the run snapshot before runner work is dispatched
  and updated as runner execution ids and cancellation outcomes become known.
  The records are the control-plane source for best-effort cleanup after
  persistence failures and run-server crashes; the runner still owns execution
  mechanics and only reports execution facts.
  """

  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AdvanceRunnerExecution
  alias FavnOrchestrator.Persistence.Commands.RecordRunnerDispatch
  alias FavnOrchestrator.Persistence.Queries.PageActiveExecutions
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RunnerExecution
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe

  @active_statuses [
    :dispatch_intent,
    :submitted,
    :started,
    :finish_persist_pending,
    :cancel_requested,
    :cancel_dispatched,
    :best_effort_failed,
    :unknown_runner_outcome
  ]

  @statuses @active_statuses ++
              [:dispatch_failed, :completed, :cancel_acknowledged, :already_completed]
  @cancel_statuses [
    :cancel_dispatched,
    :cancel_acknowledged,
    :already_completed,
    :best_effort_failed,
    :unknown_runner_outcome
  ]

  @type status ::
          :dispatch_intent
          | :submitted
          | :started
          | :finish_persist_pending
          | :dispatch_failed
          | :completed
          | :cancel_requested
          | :cancel_dispatched
          | :cancel_acknowledged
          | :already_completed
          | :best_effort_failed
          | :unknown_runner_outcome

  @type cancel_status ::
          :cancel_dispatched
          | :cancel_acknowledged
          | :already_completed
          | :best_effort_failed
          | :unknown_runner_outcome

  @type t :: %__MODULE__{
          ownership_id: String.t(),
          workspace_id: String.t() | nil,
          run_id: String.t(),
          asset_step_id: String.t(),
          node_key: term(),
          asset_ref: term(),
          stage: non_neg_integer() | nil,
          attempt: pos_integer() | nil,
          execution_pool: atom() | String.t() | nil,
          runner_execution_id: String.t() | nil,
          owner_id: String.t() | nil,
          fencing_token: pos_integer() | nil,
          persistence_version: pos_integer() | nil,
          runner_ref: term(),
          dispatch_id: String.t(),
          deadline_at: DateTime.t() | nil,
          cancel_requested_at: DateTime.t() | nil,
          cancel_outcome: map() | nil,
          status: status(),
          cancel_status: cancel_status() | nil,
          cancel_reason: term(),
          last_error: term(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @enforce_keys [:ownership_id, :run_id, :asset_step_id, :inserted_at, :updated_at]
  defstruct [
    :ownership_id,
    :workspace_id,
    :run_id,
    :asset_step_id,
    :node_key,
    :asset_ref,
    :stage,
    :attempt,
    :execution_pool,
    :runner_execution_id,
    :owner_id,
    :fencing_token,
    :persistence_version,
    :runner_ref,
    :dispatch_id,
    :deadline_at,
    :cancel_requested_at,
    :cancel_outcome,
    :cancel_status,
    :cancel_reason,
    :last_error,
    :inserted_at,
    :updated_at,
    status: :dispatch_intent
  ]

  @doc "Builds a dispatch-intent ownership record for one asset-step attempt."
  @spec new(RunState.t(), keyword()) :: t()
  def new(%RunState{id: run_id} = run, attrs) when is_list(attrs) do
    asset_step_id = Keyword.fetch!(attrs, :asset_step_id)
    attempt = Keyword.get(attrs, :attempt)
    now = DateTime.utc_now()

    ownership_id = ownership_id(run_id, asset_step_id, attempt)

    %__MODULE__{
      ownership_id: ownership_id,
      workspace_id: run.workspace_id,
      run_id: run_id,
      asset_step_id: asset_step_id,
      node_key: Keyword.get(attrs, :node_key),
      asset_ref: Keyword.get(attrs, :asset_ref),
      stage: Keyword.get(attrs, :stage),
      attempt: attempt,
      execution_pool: Keyword.get(attrs, :execution_pool),
      runner_ref: Keyword.get(attrs, :runner_ref),
      dispatch_id: Keyword.get(attrs, :dispatch_id, ownership_id),
      owner_id: run.storage_owner_id,
      fencing_token: run.storage_fencing_token,
      deadline_at: Keyword.get(attrs, :deadline_at),
      inserted_at: now,
      updated_at: now
    }
  end

  @doc "Returns true when an ownership record still represents active runner work."
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{status: status}), do: status in @active_statuses

  @doc "Returns statuses that represent active runner work or uncertain cleanup."
  @spec active_statuses() :: [status()]
  def active_statuses, do: @active_statuses

  @doc "Fetches active ownership records under the run's workspace authority."
  @spec fetch_active(RunState.t()) :: {:ok, [t()]} | {:error, term()}
  def fetch_active(%RunState{workspace_id: workspace_id} = run) when is_binary(workspace_id),
    do: fetch_active_v2(run)

  def fetch_active(%RunState{}), do: {:error, :workspace_id_required}

  @doc "Persists one ownership record in the durable storage ledger."
  @spec persist(t()) :: :ok | {:error, term()}
  def persist(%__MODULE__{workspace_id: workspace_id} = ownership)
      when is_binary(workspace_id),
      do: persist_v2(touch(ownership))

  def persist(%__MODULE__{}), do: {:error, :workspace_id_required}

  @doc "Marks a storage-ledger ownership record as submitted."
  @spec submitted(t(), String.t()) :: t()
  def submitted(%__MODULE__{} = ownership, execution_id) when is_binary(execution_id) do
    persistence_version = if ownership.workspace_id, do: 1, else: ownership.persistence_version

    %{
      ownership
      | runner_execution_id: execution_id,
        persistence_version: persistence_version,
        status: :submitted,
        last_error: nil
    }
    |> touch()
  end

  @doc "Marks a storage-ledger ownership record as started."
  @spec started(t()) :: t()
  def started(%__MODULE__{} = ownership) do
    %{ownership | status: :started, last_error: nil} |> touch()
  end

  @doc "Marks a storage-ledger ownership record as waiting on durable terminal persistence."
  @spec finish_persist_pending(t()) :: t()
  def finish_persist_pending(%__MODULE__{} = ownership) do
    %{ownership | status: :finish_persist_pending, last_error: nil} |> touch()
  end

  @doc "Marks a storage-ledger ownership record as completed."
  @spec completed(t()) :: t()
  def completed(%__MODULE__{} = ownership) do
    %{ownership | status: :completed, last_error: nil} |> touch()
  end

  @doc "Marks a dispatch intent as failed before runner ownership was established."
  @spec dispatch_failed(t(), term()) :: t()
  def dispatch_failed(%__MODULE__{} = ownership, reason) do
    %{ownership | status: :dispatch_failed, last_error: safe_diagnostic(reason)} |> touch()
  end

  @doc "Marks matching pre-submit ownership records as failed."
  @spec mark_dispatch_failed(RunState.t(), String.t(), term()) ::
          :ok | {:error, term()}
  def mark_dispatch_failed(run, ownership_id, reason) when is_binary(ownership_id) do
    with {:ok, ownerships} <- fetch_active(run) do
      ownerships
      |> Enum.filter(&(&1.ownership_id == ownership_id))
      |> persist_all(&dispatch_failed(&1, reason))
    end
  end

  @doc "Persists a terminal outcome for work whose submitted ownership write failed."
  @spec mark_submit_persist_failed(t(), map() | nil, term()) :: :ok | {:error, term()}
  def mark_submit_persist_failed(%__MODULE__{} = ownership, result, reason) do
    result = submit_persist_failure_result(ownership, result)

    ownership
    |> cancelled(result, reason)
    |> persist()
  end

  @doc "Marks matching active storage-ledger ownership records as awaiting durable persistence."
  @spec mark_finish_persist_pending(RunState.t(), String.t()) ::
          :ok | {:error, term()}
  def mark_finish_persist_pending(run, execution_id) when is_binary(execution_id) do
    with {:ok, ownerships} <- fetch_active(run) do
      ownerships
      |> Enum.filter(&(&1.runner_execution_id == execution_id))
      |> persist_all(&finish_persist_pending/1)
    end
  end

  @doc "Marks matching active storage-ledger ownership records as completed."
  @spec complete_execution(RunState.t(), String.t()) :: :ok | {:error, term()}
  def complete_execution(run, execution_id) when is_binary(execution_id) do
    with {:ok, ownerships} <- fetch_active(run) do
      ownerships
      |> Enum.filter(&(&1.runner_execution_id == execution_id))
      |> persist_all(&completed/1)
    end
  end

  @doc "Marks all active storage-ledger ownership records for a run as completed."
  @spec complete_active(RunState.t()) :: :ok | {:error, term()}
  def complete_active(run) do
    with {:ok, ownerships} <- fetch_active(run) do
      ownerships
      |> Enum.filter(&(&1.status in [:submitted, :started, :finish_persist_pending]))
      |> persist_all(&completed/1)
    end
  end

  @doc "Marks storage-ledger ownerships with cancellation outcomes."
  @spec persist_cancel_outcomes(RunState.t(), [map()], term()) ::
          :ok | {:error, term()}
  def persist_cancel_outcomes(run, results, reason) when is_list(results) do
    with {:ok, active} <- fetch_active(run) do
      active_by_execution_id = Enum.group_by(active, & &1.runner_execution_id)

      results
      |> Enum.reduce(%{}, fn result, acc ->
        case Map.get(result, :execution_id) do
          execution_id when is_binary(execution_id) -> Map.put(acc, execution_id, result)
          _invalid -> acc
        end
      end)
      |> Enum.flat_map(fn {execution_id, result} ->
        active_by_execution_id
        |> Map.get(execution_id, [])
        |> Enum.map(&cancelled(&1, result, reason))
      end)
      |> persist_all()
    end
  end

  @doc "Marks active storage-ledger ownerships without runner execution ids as unknown."
  @spec persist_unknown_without_execution_id(RunState.t(), term()) ::
          :ok | {:error, term()}
  def persist_unknown_without_execution_id(run, reason) do
    with {:ok, ownerships} <- fetch_active(run) do
      reason = safe_diagnostic(reason)

      ownerships
      |> Enum.filter(&is_nil(&1.runner_execution_id))
      |> Enum.map(fn ownership ->
        %{
          ownership
          | status: :unknown_runner_outcome,
            cancel_status: :unknown_runner_outcome,
            cancel_reason: reason,
            last_error: :missing_runner_execution_id
        }
        |> touch()
      end)
      |> persist_all()
    end
  end

  @doc false
  @spec valid_status?(term()) :: boolean()
  def valid_status?(status), do: status in @statuses

  @doc false
  @spec valid_cancel_status?(term()) :: boolean()
  def valid_cancel_status?(nil), do: true
  def valid_cancel_status?(status), do: status in @cancel_statuses

  @doc false
  @spec normalize_status(term()) :: {:ok, status()} | {:error, term()}
  def normalize_status(nil), do: {:ok, :dispatch_intent}
  def normalize_status(status) when status in @statuses, do: {:ok, status}

  def normalize_status(status) when is_binary(status) do
    case Enum.find(@statuses, &(Atom.to_string(&1) == status)) do
      nil -> {:error, {:invalid_execution_ownership_field, :status}}
      normalized -> {:ok, normalized}
    end
  end

  def normalize_status(_status), do: {:error, {:invalid_execution_ownership_field, :status}}

  @doc false
  @spec normalize_cancel_status(term()) :: {:ok, cancel_status() | nil} | {:error, term()}
  def normalize_cancel_status(nil), do: {:ok, nil}
  def normalize_cancel_status(status) when status in @cancel_statuses, do: {:ok, status}

  def normalize_cancel_status(status) when is_binary(status) do
    case Enum.find(@cancel_statuses, &(Atom.to_string(&1) == status)) do
      nil -> {:error, {:invalid_execution_ownership_field, :cancel_status}}
      normalized -> {:ok, normalized}
    end
  end

  def normalize_cancel_status(_status),
    do: {:error, {:invalid_execution_ownership_field, :cancel_status}}

  @doc "Maps a runner cancellation dispatch result to an ownership cancellation status."
  @spec cancel_outcome_status(map()) :: cancel_status()
  def cancel_outcome_status(%{status: :acknowledged}), do: :cancel_acknowledged
  def cancel_outcome_status(%{status: :already_completed}), do: :already_completed
  def cancel_outcome_status(%{status: :not_found}), do: :unknown_runner_outcome
  def cancel_outcome_status(%{error: _error}), do: :best_effort_failed
  def cancel_outcome_status(_result), do: :cancel_dispatched

  defp fetch_active_v2(%RunState{} = run) do
    context = SystemContext.workspace(run.workspace_id, :runner_execution_recovery)
    fetch_active_pages(context, run, nil, [])
  end

  defp fetch_active_pages(context, run, cursor, acc) do
    query = %PageActiveExecutions{
      workspace_context: context,
      run_id: run.id,
      after: cursor,
      limit: 500
    }

    case Persistence.stores().run_ownership.page_active_executions(query) do
      {:ok, %CursorPage{} = page} ->
        ownerships = Enum.map(page.items, &from_runner_execution(&1, run))
        acc = [ownerships | acc]

        if page.has_more? do
          fetch_active_pages(context, run, page.next_cursor, acc)
        else
          {:ok, acc |> Enum.reverse() |> List.flatten()}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_v2(%__MODULE__{status: :dispatch_intent} = ownership) do
    with :ok <- validate_v2_authority(ownership),
         {:ok, _execution} <-
           Persistence.stores().run_ownership.record_dispatch(%RecordRunnerDispatch{
             workspace_context: context(ownership, :runner_dispatch),
             command_id: command_id("dispatch", ownership.dispatch_id, 1),
             run_id: ownership.run_id,
             runner_execution_id: ownership.dispatch_id,
             dispatch_id: ownership.dispatch_id,
             owner_id: ownership.owner_id,
             fencing_token: ownership.fencing_token,
             payload: dispatch_payload(ownership),
             occurred_at: ownership.updated_at
           }) do
      :ok
    end
  end

  defp persist_v2(%__MODULE__{status: status})
       when status in [:submitted, :finish_persist_pending],
       do: :ok

  defp persist_v2(%__MODULE__{} = ownership) do
    with :ok <- validate_v2_authority(ownership),
         {:ok, status, result, error} <- v2_transition(ownership),
         {:ok, _execution} <-
           Persistence.stores().run_ownership.advance_execution(%AdvanceRunnerExecution{
             workspace_context: context(ownership, :runner_execution_transition),
             command_id:
               command_id(
                 Atom.to_string(status),
                 ownership.runner_execution_id || ownership.dispatch_id,
                 ownership.persistence_version || 1
               ),
             run_id: ownership.run_id,
             runner_execution_id: ownership.runner_execution_id || ownership.dispatch_id,
             owner_id: ownership.owner_id,
             fencing_token: ownership.fencing_token,
             expected_version: ownership.persistence_version || 1,
             status: status,
             result: result,
             error: error,
             occurred_at: ownership.updated_at
           }) do
      :ok
    end
  end

  defp validate_v2_authority(%__MODULE__{
         workspace_id: workspace_id,
         run_id: run_id,
         dispatch_id: dispatch_id,
         owner_id: owner_id,
         fencing_token: fencing_token
       })
       when is_binary(workspace_id) and workspace_id != "" and is_binary(run_id) and run_id != "" and
              is_binary(dispatch_id) and dispatch_id != "" and is_binary(owner_id) and
              owner_id != "" and
              is_integer(fencing_token) and fencing_token > 0,
       do: :ok

  defp validate_v2_authority(%__MODULE__{}),
    do: {:error, {:invalid_runner_execution_authority, :missing_run_fence}}

  defp v2_transition(%__MODULE__{status: :started}), do: {:ok, :running, nil, nil}

  defp v2_transition(%__MODULE__{status: :completed}),
    do: {:ok, :ok, %{"outcome" => "orchestrator_persisted"}, nil}

  defp v2_transition(%__MODULE__{status: :already_completed}),
    do: {:ok, :cancelled, nil, nil}

  defp v2_transition(%__MODULE__{status: status})
       when status in [:cancel_requested, :cancel_dispatched],
       do: {:ok, :cancelling, nil, nil}

  defp v2_transition(%__MODULE__{status: status})
       when status in [:cancel_acknowledged],
       do: {:ok, :cancelled, nil, nil}

  defp v2_transition(%__MODULE__{status: status} = ownership)
       when status in [:dispatch_failed, :best_effort_failed, :unknown_runner_outcome] do
    {:ok, :error, nil, JsonSafe.error(ownership.last_error || ownership.cancel_reason || status)}
  end

  defp v2_transition(%__MODULE__{status: status}),
    do: {:error, {:invalid_runner_execution_transition, status}}

  defp dispatch_payload(%__MODULE__{} = ownership) do
    JsonSafe.data(%{
      ownership_id: ownership.ownership_id,
      asset_step_id: ownership.asset_step_id,
      node_key: ownership.node_key,
      asset_ref: ownership.asset_ref,
      stage: ownership.stage,
      attempt: ownership.attempt,
      execution_pool: ownership.execution_pool,
      runner_ref: ownership.runner_ref,
      deadline_at: ownership.deadline_at
    })
  end

  defp from_runner_execution(%RunnerExecution{} = execution, %RunState{} = run) do
    payload = execution.payload || %{}

    %__MODULE__{
      ownership_id: payload_value(payload, "ownership_id") || execution.dispatch_id,
      workspace_id: execution.workspace_id,
      run_id: execution.run_id,
      asset_step_id: payload_value(payload, "asset_step_id") || execution.dispatch_id,
      node_key: payload_value(payload, "node_key"),
      asset_ref: payload_value(payload, "asset_ref"),
      stage: payload_value(payload, "stage"),
      attempt: payload_value(payload, "attempt"),
      execution_pool: payload_value(payload, "execution_pool"),
      runner_execution_id: execution.runner_execution_id,
      runner_ref: payload_value(payload, "runner_ref"),
      dispatch_id: execution.dispatch_id,
      owner_id: run.storage_owner_id || execution.owner_id,
      fencing_token: run.storage_fencing_token || execution.fencing_token,
      persistence_version: execution.version,
      deadline_at: parse_datetime(payload_value(payload, "deadline_at")),
      status: legacy_status(execution.status),
      inserted_at: execution.dispatched_at || DateTime.utc_now(),
      updated_at: execution.terminal_at || execution.dispatched_at || DateTime.utc_now()
    }
  end

  defp legacy_status(:dispatching), do: :submitted
  defp legacy_status(:running), do: :started
  defp legacy_status(:cancelling), do: :cancel_dispatched

  defp payload_value(payload, key),
    do: Map.get(payload, key, Map.get(payload, String.to_existing_atom(key)))

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _invalid -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp context(%__MODULE__{} = ownership, purpose),
    do: SystemContext.workspace(ownership.workspace_id, purpose)

  defp command_id(operation, identity, version) do
    digest =
      :crypto.hash(:sha256, :erlang.term_to_binary({operation, identity, version}))
      |> Base.url_encode64(padding: false)

    "runner:#{operation}:#{digest}"
  end

  defp cancelled(%__MODULE__{} = ownership, result, reason) do
    status = cancel_outcome_status(result)

    %{
      ownership
      | status: status,
        cancel_status: status,
        cancel_requested_at: DateTime.utc_now(),
        cancel_outcome:
          Map.take(result, [
            :execution_id,
            :status,
            :runner_status,
            :native_status,
            :reason_class,
            :correlation_id
          ]),
        cancel_reason: safe_diagnostic(reason),
        last_error: safe_diagnostic(Map.get(result, :error))
    }
    |> touch()
  end

  defp submit_persist_failure_result(%__MODULE__{} = ownership, %{} = result) do
    Map.put_new(result, :execution_id, ownership.runner_execution_id)
  end

  defp submit_persist_failure_result(%__MODULE__{} = ownership, _result) do
    %{
      execution_id: ownership.runner_execution_id,
      status: :unknown_runner_outcome,
      error: :missing_cancel_outcome
    }
  end

  defp persist_all(ownerships, mapper \\ & &1) when is_list(ownerships) do
    Enum.reduce_while(ownerships, :ok, fn ownership, :ok ->
      case persist(mapper.(ownership)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ownership_id(run_id, asset_step_id, attempt) do
    Enum.join([run_id, asset_step_id, attempt || 1], ":")
  end

  defp touch(%__MODULE__{} = ownership), do: %{ownership | updated_at: DateTime.utc_now()}

  defp safe_diagnostic(nil), do: nil

  defp safe_diagnostic(value) do
    case Redaction.redact_operational_bounded(%{error: value}) do
      %{error: safe} -> safe
      _other -> "[REDACTED]"
    end
  end
end
