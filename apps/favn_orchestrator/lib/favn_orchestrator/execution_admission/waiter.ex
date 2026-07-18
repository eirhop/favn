defmodule FavnOrchestrator.ExecutionAdmission.Waiter do
  @moduledoc """
  Persisted admission waiter for one deferred asset step.

  Waiters record queued admission intent durably. They do not grant capacity;
  run servers must still acquire a storage-backed execution lease before
  submitting runner work.
  """

  alias FavnOrchestrator.ExecutionAdmission.Identity
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  @type queue_reason :: :pipeline_concurrency | :execution_pool | :global_concurrency
  @type scope :: %{
          required(:kind) => atom() | String.t(),
          required(:key) => String.t(),
          required(:limit) => pos_integer()
        }

  @type t :: %__MODULE__{
          workspace_id: String.t() | nil,
          waiter_id: String.t(),
          run_id: String.t(),
          asset_step_id: String.t(),
          queue_reason: queue_reason(),
          blocked_scope: scope(),
          requested_scopes: [scope()],
          stage: non_neg_integer(),
          attempt: pos_integer(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t(),
          deadline_at: DateTime.t() | nil,
          wake_generation: non_neg_integer()
        }

  @enforce_keys [
    :waiter_id,
    :run_id,
    :asset_step_id,
    :queue_reason,
    :blocked_scope,
    :requested_scopes,
    :stage,
    :attempt,
    :inserted_at,
    :updated_at,
    :wake_generation
  ]
  defstruct [
    :workspace_id,
    :waiter_id,
    :run_id,
    :asset_step_id,
    :queue_reason,
    :blocked_scope,
    :requested_scopes,
    :stage,
    :attempt,
    :inserted_at,
    :updated_at,
    :deadline_at,
    :wake_generation
  ]

  @doc "Builds a normalized admission waiter."
  @spec new(RunState.t(), map(), [scope()], queue_reason(), scope(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def new(%RunState{} = run, entry, requested_scopes, queue_reason, blocked_scope, opts \\ [])
      when is_map(entry) and is_list(requested_scopes) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    asset_step_id = Map.get(entry, :asset_step_id) || Map.get(entry, "asset_step_id")

    normalize(%{
      waiter_id:
        waiter_id(
          run.id,
          asset_step_id,
          Keyword.get(opts, :stage, 0),
          Keyword.get(opts, :attempt, 1)
        ),
      run_id: run.id,
      asset_step_id: asset_step_id,
      queue_reason: queue_reason,
      blocked_scope: blocked_scope,
      requested_scopes: requested_scopes,
      stage: Keyword.get(opts, :stage, 0),
      attempt: Keyword.get(opts, :attempt, 1),
      inserted_at: now,
      updated_at: now,
      deadline_at: Keyword.get(opts, :deadline_at, default_deadline(run, now)),
      wake_generation: Keyword.get(opts, :wake_generation, 0)
    })
  end

  @doc "Normalizes atom-keyed or string-keyed waiter maps."
  @spec normalize(map() | t()) :: {:ok, t()} | {:error, term()}
  def normalize(%__MODULE__{} = waiter), do: waiter |> Map.from_struct() |> normalize()

  def normalize(waiter) when is_map(waiter) do
    with {:ok, waiter_id} <- fetch_string(waiter, :waiter_id),
         {:ok, run_id} <- fetch_string(waiter, :run_id),
         {:ok, asset_step_id} <- fetch_string(waiter, :asset_step_id),
         {:ok, queue_reason} <- normalize_queue_reason(field_value(waiter, :queue_reason)),
         {:ok, blocked_scope} <-
           ExecutionLeaseCodec.normalize_scope(field_value(waiter, :blocked_scope)),
         {:ok, requested_scopes} <- normalize_scopes(field_value(waiter, :requested_scopes)),
         :ok <- ensure_blocked_scope_requested(blocked_scope, requested_scopes),
         :ok <- ensure_queue_reason_scope(queue_reason, blocked_scope),
         {:ok, stage} <- fetch_non_neg_integer(waiter, :stage),
         {:ok, attempt} <- fetch_positive_integer(waiter, :attempt),
         {:ok, inserted_at} <- fetch_datetime(waiter, :inserted_at),
         {:ok, updated_at} <- fetch_datetime(waiter, :updated_at),
         {:ok, deadline_at} <- fetch_optional_datetime(waiter, :deadline_at),
         {:ok, wake_generation} <- fetch_non_neg_integer(waiter, :wake_generation) do
      {:ok,
       %__MODULE__{
         workspace_id: optional_string(field_value(waiter, :workspace_id)),
         waiter_id: waiter_id,
         run_id: run_id,
         asset_step_id: asset_step_id,
         queue_reason: queue_reason,
         blocked_scope: blocked_scope,
         requested_scopes: requested_scopes,
         stage: stage,
         attempt: attempt,
         inserted_at: inserted_at,
         updated_at: updated_at,
         deadline_at: deadline_at,
         wake_generation: wake_generation
       }}
    end
  end

  def normalize(_waiter), do: {:error, :invalid_execution_admission_waiter}

  @doc "Returns the deterministic waiter id for one run stage entry."
  @spec waiter_id(String.t(), String.t(), non_neg_integer(), pos_integer()) :: String.t()
  def waiter_id(run_id, asset_step_id, stage, attempt)
      when is_binary(run_id) and is_binary(asset_step_id) and is_integer(stage) and
             stage >= 0 and is_integer(attempt) and attempt > 0,
      do: Identity.waiter_id(run_id, asset_step_id, stage, attempt)

  defp default_deadline(%RunState{timeout_ms: timeout_ms}, %DateTime{} = now)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    DateTime.add(now, timeout_ms + 2_000, :millisecond)
  end

  defp default_deadline(%RunState{}, _now), do: nil

  defp normalize_scopes(scopes) when is_list(scopes) and scopes != [] do
    scopes
    |> Enum.reduce_while({:ok, []}, fn scope, {:ok, acc} ->
      case ExecutionLeaseCodec.normalize_scope(scope) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_scopes(_scopes), do: {:error, :invalid_execution_admission_waiter_scopes}

  defp ensure_blocked_scope_requested(blocked_scope, requested_scopes) do
    blocked_identity = ExecutionLeaseCodec.scope_identity(blocked_scope)

    if Enum.any?(requested_scopes, &(ExecutionLeaseCodec.scope_identity(&1) == blocked_identity)),
      do: :ok,
      else: {:error, {:invalid_execution_admission_waiter_field, :blocked_scope}}
  end

  defp ensure_queue_reason_scope(:pipeline_concurrency, %{kind: :run}), do: :ok
  defp ensure_queue_reason_scope(:global_concurrency, %{kind: :global}), do: :ok
  defp ensure_queue_reason_scope(:execution_pool, %{kind: :pool}), do: :ok

  defp ensure_queue_reason_scope(_reason, _scope),
    do: {:error, {:invalid_execution_admission_waiter_field, :queue_reason}}

  defp normalize_queue_reason(reason)
       when reason in [:pipeline_concurrency, :execution_pool, :global_concurrency],
       do: {:ok, reason}

  defp normalize_queue_reason(reason) when is_binary(reason) do
    case reason do
      "pipeline_concurrency" -> {:ok, :pipeline_concurrency}
      "execution_pool" -> {:ok, :execution_pool}
      "global_concurrency" -> {:ok, :global_concurrency}
      _other -> {:error, {:invalid_execution_admission_waiter_field, :queue_reason}}
    end
  end

  defp normalize_queue_reason(_reason),
    do: {:error, {:invalid_execution_admission_waiter_field, :queue_reason}}

  defp fetch_string(map, field) do
    case field_value(map, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_admission_waiter_field, field}}
    end
  end

  defp fetch_positive_integer(map, field) do
    case field_value(map, field) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, {:invalid_execution_admission_waiter_field, field}}
    end
  end

  defp fetch_non_neg_integer(map, field) do
    case field_value(map, field) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, {:invalid_execution_admission_waiter_field, field}}
    end
  end

  defp fetch_datetime(map, field) do
    case field_value(map, field) do
      %DateTime{} = value -> {:ok, value}
      _other -> {:error, {:invalid_execution_admission_waiter_field, field}}
    end
  end

  defp fetch_optional_datetime(map, field) do
    case field_value(map, field) do
      nil -> {:ok, nil}
      %DateTime{} = value -> {:ok, value}
      _other -> {:error, {:invalid_execution_admission_waiter_field, field}}
    end
  end

  defp field_value(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(field))
    end
  end

  defp optional_string(value) when is_binary(value) and value != "", do: value
  defp optional_string(_value), do: nil
end
