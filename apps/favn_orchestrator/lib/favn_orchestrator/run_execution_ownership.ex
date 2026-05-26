defmodule FavnOrchestrator.RunExecutionOwnership do
  @moduledoc """
  Orchestrator-owned runner execution ownership records.

  Ownership is persisted with the run snapshot before runner work is dispatched
  and updated as runner execution ids and cancellation outcomes become known.
  The records are the control-plane source for best-effort cleanup after
  persistence failures and run-server crashes; the runner still owns execution
  mechanics and only reports execution facts.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

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
          run_id: String.t(),
          asset_step_id: String.t(),
          node_key: term(),
          asset_ref: term(),
          stage: non_neg_integer() | nil,
          attempt: pos_integer() | nil,
          execution_pool: atom() | String.t() | nil,
          runner_execution_id: String.t() | nil,
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
    :run_id,
    :asset_step_id,
    :node_key,
    :asset_ref,
    :stage,
    :attempt,
    :execution_pool,
    :runner_execution_id,
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
  def new(%RunState{id: run_id}, attrs) when is_list(attrs) do
    asset_step_id = Keyword.fetch!(attrs, :asset_step_id)
    attempt = Keyword.get(attrs, :attempt)
    now = DateTime.utc_now()

    ownership_id = ownership_id(run_id, asset_step_id, attempt)

    %__MODULE__{
      ownership_id: ownership_id,
      run_id: run_id,
      asset_step_id: asset_step_id,
      node_key: Keyword.get(attrs, :node_key),
      asset_ref: Keyword.get(attrs, :asset_ref),
      stage: Keyword.get(attrs, :stage),
      attempt: attempt,
      execution_pool: Keyword.get(attrs, :execution_pool),
      runner_ref: Keyword.get(attrs, :runner_ref),
      dispatch_id: Keyword.get(attrs, :dispatch_id, ownership_id),
      deadline_at: Keyword.get(attrs, :deadline_at),
      inserted_at: now,
      updated_at: now
    }
  end

  @doc "Returns active ownership records persisted in the storage ledger."
  @spec active(RunState.t()) :: [t()]
  def active(%RunState{id: run_id}), do: active(run_id)

  @spec active(String.t()) :: [t()]
  def active(run_id) when is_binary(run_id) do
    case Storage.list_active_execution_ownerships(run_id) do
      {:ok, ownerships} -> ownerships
      {:error, _reason} -> []
    end
  end

  @doc "Returns true when an ownership record still represents active runner work."
  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{status: status}), do: status in @active_statuses

  @doc "Returns statuses that represent active runner work or uncertain cleanup."
  @spec active_statuses() :: [status()]
  def active_statuses, do: @active_statuses

  @doc "Fetches active ownership records, preserving storage read errors."
  @spec fetch_active(String.t()) :: {:ok, [t()]} | {:error, term()}
  def fetch_active(run_id) when is_binary(run_id),
    do: Storage.list_active_execution_ownerships(run_id)

  @spec fetch_active(RunState.t()) :: {:ok, [t()]} | {:error, term()}
  def fetch_active(%RunState{id: run_id}), do: fetch_active(run_id)

  @doc "Returns known active runner execution ids for a run."
  @spec active_execution_ids(RunState.t()) :: [String.t()]
  def active_execution_ids(%RunState{} = run_state) do
    run_state
    |> active()
    |> Enum.map(& &1.runner_execution_id)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  @spec active_execution_ids(String.t()) :: [String.t()]
  def active_execution_ids(run_id) when is_binary(run_id) do
    run_id
    |> active()
    |> Enum.map(& &1.runner_execution_id)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  @doc "Persists one ownership record in the durable storage ledger."
  @spec persist(t()) :: :ok | {:error, term()}
  def persist(%__MODULE__{} = ownership), do: Storage.put_execution_ownership(touch(ownership))

  @doc "Marks a storage-ledger ownership record as submitted."
  @spec submitted(t(), String.t()) :: t()
  def submitted(%__MODULE__{} = ownership, execution_id) when is_binary(execution_id) do
    %{ownership | runner_execution_id: execution_id, status: :submitted, last_error: nil}
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
    %{ownership | status: :dispatch_failed, last_error: reason} |> touch()
  end

  @doc "Marks matching pre-submit ownership records as failed."
  @spec mark_dispatch_failed(String.t(), String.t(), term()) :: :ok | {:error, term()}
  def mark_dispatch_failed(run_id, ownership_id, reason)
      when is_binary(run_id) and is_binary(ownership_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
      ownerships
      |> Enum.filter(&(&1.ownership_id == ownership_id))
      |> persist_all(&dispatch_failed(&1, reason))
    end
  end

  @doc "Marks matching active storage-ledger ownership records as awaiting durable persistence."
  @spec mark_finish_persist_pending(String.t(), String.t()) :: :ok | {:error, term()}
  def mark_finish_persist_pending(run_id, execution_id)
      when is_binary(run_id) and is_binary(execution_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
      ownerships
      |> Enum.filter(&(&1.runner_execution_id == execution_id))
      |> persist_all(&finish_persist_pending/1)
    end
  end

  @doc "Marks matching active storage-ledger ownership records as completed."
  @spec complete_execution(String.t(), String.t()) :: :ok | {:error, term()}
  def complete_execution(run_id, execution_id)
      when is_binary(run_id) and is_binary(execution_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
      ownerships
      |> Enum.filter(&(&1.runner_execution_id == execution_id))
      |> persist_all(&completed/1)
    end
  end

  @doc "Marks all active storage-ledger ownership records for a run as completed."
  @spec complete_active(String.t()) :: :ok | {:error, term()}
  def complete_active(run_id) when is_binary(run_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
      ownerships
      |> Enum.filter(&(&1.status in [:submitted, :started, :finish_persist_pending]))
      |> persist_all(&completed/1)
    end
  end

  @doc "Marks storage-ledger ownerships with cancellation outcomes."
  @spec persist_cancel_outcomes(String.t(), [map()], term()) :: :ok | {:error, term()}
  def persist_cancel_outcomes(run_id, results, reason)
      when is_binary(run_id) and is_list(results) do
    with {:ok, active} <- fetch_active(run_id) do
      results
      |> Enum.flat_map(fn result ->
        execution_id = Map.get(result, :execution_id)

        Enum.filter(active, &(&1.runner_execution_id == execution_id))
        |> Enum.map(&cancelled(&1, result, reason))
      end)
      |> persist_all()
    end
  end

  @doc "Marks active storage-ledger ownerships without runner execution ids as unknown."
  @spec persist_unknown_without_execution_id(String.t(), term()) :: :ok | {:error, term()}
  def persist_unknown_without_execution_id(run_id, reason) when is_binary(run_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
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

  @doc "Fetches all durable ownership records for a run, preserving storage read errors."
  @spec fetch_all(String.t()) :: {:ok, [t()]} | {:error, term()}
  def fetch_all(run_id) when is_binary(run_id), do: Storage.list_execution_ownerships(run_id)

  @doc "Returns all durable ownership records for a run."
  @spec list(RunState.t()) :: [t()]
  def list(%RunState{id: run_id}), do: list(run_id)

  @spec list(String.t()) :: [t()]
  def list(run_id) when is_binary(run_id) do
    case Storage.list_execution_ownerships(run_id) do
      {:ok, ownerships} -> ownerships
      {:error, _reason} -> []
    end
  end

  @doc "Returns a JSON-safe-ish map for run-event data."
  @spec event_data(t()) :: map()
  def event_data(%__MODULE__{} = ownership) do
    ownership
    |> to_map()
    |> Map.take([
      :ownership_id,
      :asset_step_id,
      :node_key,
      :asset_ref,
      :stage,
      :attempt,
      :execution_pool,
      :runner_execution_id,
      :runner_ref,
      :dispatch_id,
      :deadline_at,
      :cancel_requested_at,
      :cancel_outcome,
      :status,
      :cancel_status,
      :last_error
    ])
  end

  @doc "Encodes an ownership record as a storage map."
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = ownership), do: to_metadata(ownership)

  @doc "Decodes one ownership record from a storage map."
  @spec from_map(map() | t()) :: {:ok, t()} | {:error, term()}
  def from_map(%__MODULE__{} = ownership), do: {:ok, ownership}

  def from_map(%{} = map) do
    case from_metadata(map) do
      [%__MODULE__{} = ownership] -> {:ok, ownership}
      [] -> {:error, :invalid_execution_ownership}
    end
  end

  def from_map(_value), do: {:error, :invalid_execution_ownership}

  defp from_metadata(%__MODULE__{} = ownership), do: [ownership]

  defp from_metadata(%{} = map) do
    status =
      known_atom(Map.get(map, :status, Map.get(map, "status")), @statuses, :dispatch_intent)

    cancel_status =
      known_atom(
        Map.get(map, :cancel_status, Map.get(map, "cancel_status")),
        @cancel_statuses,
        nil
      )

    with ownership_id when is_binary(ownership_id) <-
           Map.get(map, :ownership_id, Map.get(map, "ownership_id")),
         run_id when is_binary(run_id) <- Map.get(map, :run_id, Map.get(map, "run_id")),
         asset_step_id when is_binary(asset_step_id) <-
           Map.get(map, :asset_step_id, Map.get(map, "asset_step_id")) do
      [
        %__MODULE__{
          ownership_id: ownership_id,
          run_id: run_id,
          asset_step_id: asset_step_id,
          node_key: value(map, :node_key),
          asset_ref: value(map, :asset_ref),
          stage: value(map, :stage),
          attempt: value(map, :attempt),
          execution_pool: value(map, :execution_pool),
          runner_execution_id: value(map, :runner_execution_id),
          runner_ref: value(map, :runner_ref),
          dispatch_id: value(map, :dispatch_id) || ownership_id,
          deadline_at: optional_datetime(value(map, :deadline_at)),
          cancel_requested_at: optional_datetime(value(map, :cancel_requested_at)),
          cancel_outcome: value(map, :cancel_outcome),
          status: status,
          cancel_status: cancel_status,
          cancel_reason: value(map, :cancel_reason),
          last_error: value(map, :last_error),
          inserted_at: datetime(value(map, :inserted_at)),
          updated_at: datetime(value(map, :updated_at))
        }
      ]
    else
      _ -> []
    end
  end

  defp from_metadata(_value), do: []

  defp to_metadata(%__MODULE__{} = ownership) do
    %{
      ownership_id: ownership.ownership_id,
      run_id: ownership.run_id,
      asset_step_id: ownership.asset_step_id,
      node_key: ownership.node_key,
      asset_ref: ownership.asset_ref,
      stage: ownership.stage,
      attempt: ownership.attempt,
      execution_pool: ownership.execution_pool,
      runner_execution_id: ownership.runner_execution_id,
      runner_ref: ownership.runner_ref,
      dispatch_id: ownership.dispatch_id,
      deadline_at: ownership.deadline_at,
      cancel_requested_at: ownership.cancel_requested_at,
      cancel_outcome: ownership.cancel_outcome,
      status: ownership.status,
      cancel_status: ownership.cancel_status,
      cancel_reason: ownership.cancel_reason,
      last_error: ownership.last_error,
      inserted_at: ownership.inserted_at,
      updated_at: ownership.updated_at
    }
  end

  @doc "Maps a runner cancellation dispatch result to an ownership cancellation status."
  @spec cancel_outcome_status(map()) :: cancel_status()
  def cancel_outcome_status(%{status: :acknowledged}), do: :cancel_acknowledged
  def cancel_outcome_status(%{status: :already_completed}), do: :already_completed
  def cancel_outcome_status(%{status: :not_found}), do: :unknown_runner_outcome
  def cancel_outcome_status(%{error: _error}), do: :best_effort_failed
  def cancel_outcome_status(_result), do: :cancel_dispatched

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
        cancel_reason: reason,
        last_error: Map.get(result, :error)
    }
    |> touch()
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

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp known_atom(nil, _allowed, default), do: default

  defp known_atom(value, allowed, default) when is_atom(value) do
    if value in allowed, do: value, else: default
  end

  defp known_atom(value, allowed, default) when is_binary(value) do
    Enum.find(allowed, default, &(Atom.to_string(&1) == value))
  end

  defp known_atom(_value, _allowed, default), do: default

  defp datetime(%DateTime{} = value), do: value

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> DateTime.utc_now()
    end
  end

  defp datetime(_value), do: DateTime.utc_now()

  defp optional_datetime(nil), do: nil
  defp optional_datetime(value), do: datetime(value)

  defp touch(%__MODULE__{} = ownership), do: %{ownership | updated_at: DateTime.utc_now()}
end
