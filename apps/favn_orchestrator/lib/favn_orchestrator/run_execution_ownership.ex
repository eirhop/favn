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
    %{ownership | status: :dispatch_failed, last_error: safe_diagnostic(reason)} |> touch()
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

  @doc "Persists a terminal outcome for work whose submitted ownership write failed."
  @spec mark_submit_persist_failed(t(), map() | nil, term()) :: :ok | {:error, term()}
  def mark_submit_persist_failed(%__MODULE__{} = ownership, result, reason) do
    result = submit_persist_failure_result(ownership, result)

    ownership
    |> cancelled(result, reason)
    |> persist()
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
  @spec persist_unknown_without_execution_id(String.t(), term()) :: :ok | {:error, term()}
  def persist_unknown_without_execution_id(run_id, reason) when is_binary(run_id) do
    with {:ok, ownerships} <- fetch_active(run_id) do
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

  @doc "Fetches all durable ownership records for a run, preserving storage read errors."
  @spec fetch_all(String.t()) :: {:ok, [t()]} | {:error, term()}
  def fetch_all(run_id) when is_binary(run_id), do: Storage.list_execution_ownerships(run_id)

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
