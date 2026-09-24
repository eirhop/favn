defmodule FavnOrchestrator.RunServer.Execution.RegistrationRetry do
  @moduledoc """
  Finite registration reconciliation budget, persisted before each retry dispatch.

  A saved slot counts even when its worker never starts. Replacement owners retain
  the deadline and count; only an explicit recovery resume starts a new epoch.
  """
  alias FavnOrchestrator.Persistence.Error

  @budget_ms 30_000
  @max_slots 8
  @enforce_keys [:asset_step_id, :task_id, :attempt, :stage, :first_failure_at, :deadline_at]
  defstruct @enforce_keys ++ [slots: 0, next_at: nil, deadline_ms: nil, reason_code: nil]
  @type t :: %__MODULE__{}

  @doc "Classifies only retryable persistence failures, including nested operation errors."
  @spec retryable?(term()) :: boolean()
  def retryable?(%Error{kind: kind, retryable?: true})
      when kind in [:conflict, :unavailable, :timeout], do: true

  def retryable?({_operation, reason}), do: retryable?(reason)
  def retryable?(_), do: false

  @doc "Reserves the next slot within the original wall-clock and monotonic bounds."
  @spec next(t() | nil, map(), term(), DateTime.t(), integer()) ::
          {:ok, t()} | {:error, :registration_retry_exhausted}
  def next(
        previous,
        pending,
        reason,
        now \\ DateTime.utc_now(),
        monotonic \\ System.monotonic_time(:millisecond)
      ) do
    retry =
      previous ||
        %__MODULE__{
          asset_step_id: pending.entry.asset_step_id,
          task_id: pending.entry.task_id,
          attempt: pending.attempt,
          stage: pending.stage,
          first_failure_at: now,
          deadline_at: DateTime.add(now, @budget_ms, :millisecond)
        }

    retry = local_deadline(retry, now, monotonic)
    remaining = remaining_ms(retry, now, monotonic)

    if retry.slots >= @max_slots or remaining <= 0 do
      {:error, :registration_retry_exhausted}
    else
      base = Enum.at([1_000, 2_000, 4_000], retry.slots, 5_000)
      jitter = :rand.uniform(div(base * 2, 5) + 1) - div(base, 5) - 1
      delay = min(base + jitter, remaining)

      {:ok,
       %{
         retry
         | slots: retry.slots + 1,
           next_at: DateTime.add(now, delay, :millisecond),
           reason_code: reason_code(reason)
       }}
    end
  end

  @doc "Returns the remaining time without extending a process-local deadline."
  @spec remaining_ms(t(), DateTime.t(), integer()) :: non_neg_integer()
  def remaining_ms(
        retry,
        now \\ DateTime.utc_now(),
        monotonic \\ System.monotonic_time(:millisecond)
      ) do
    wall = DateTime.diff(retry.deadline_at, now, :millisecond)
    max(min(wall, if(retry.deadline_ms, do: retry.deadline_ms - monotonic, else: wall)), 0)
  end

  @doc "Encodes the compact versioned event; process clock values are never persisted."
  @spec event(t()) :: map()
  def event(retry) do
    %{
      "version" => 1,
      "asset_step_id" => retry.asset_step_id,
      "runner_task_id" => retry.task_id,
      "attempt" => retry.attempt,
      "stage" => retry.stage,
      "scheduled_retries" => retry.slots,
      "first_failure_at" => DateTime.to_iso8601(retry.first_failure_at),
      "deadline_at" => DateTime.to_iso8601(retry.deadline_at),
      "next_at" => DateTime.to_iso8601(retry.next_at),
      "reason_code" => retry.reason_code
    }
  end

  @doc "Validates a saved slot against the original outcome and preceding slot."
  @spec restore(map(), map() | nil, t() | nil) :: {:ok, t()} | {:error, atom()}
  def restore(data, step, previous) do
    with 1 <- data["version"],
         %{phase: :outcome, task_id: task, attempt: attempt, stage: stage, status: :ok} <- step,
         true <-
           task == data["runner_task_id"] and attempt == data["attempt"] and
             stage == data["stage"],
         slots when is_integer(slots) and slots in 1..@max_slots <- data["scheduled_retries"],
         true <- slots == if(previous, do: previous.slots + 1, else: 1),
         {:ok, first, 0} <- DateTime.from_iso8601(data["first_failure_at"]),
         {:ok, deadline, 0} <- DateTime.from_iso8601(data["deadline_at"]),
         {:ok, next_at, 0} <- DateTime.from_iso8601(data["next_at"]),
         true <- DateTime.diff(deadline, first, :millisecond) == @budget_ms,
         true <-
           DateTime.compare(next_at, first) != :lt and DateTime.compare(next_at, deadline) != :gt,
         true <-
           is_nil(previous) or
             (previous.first_failure_at == first and previous.deadline_at == deadline),
         code when code in ["conflict", "unavailable", "timeout"] <- data["reason_code"] do
      {:ok,
       local_deadline(
         %__MODULE__{
           asset_step_id: data["asset_step_id"],
           task_id: task,
           attempt: attempt,
           stage: stage,
           slots: slots,
           first_failure_at: first,
           deadline_at: deadline,
           next_at: next_at,
           reason_code: code
         },
         DateTime.utc_now(),
         System.monotonic_time(:millisecond)
       )}
    else
      _ -> {:error, :invalid_registration_retry_event}
    end
  rescue
    _ -> {:error, :invalid_registration_retry_event}
  end

  defp local_deadline(retry, now, monotonic) do
    deadline = monotonic + max(DateTime.diff(retry.deadline_at, now, :millisecond), 0)

    %{
      retry
      | deadline_ms: if(retry.deadline_ms, do: min(retry.deadline_ms, deadline), else: deadline)
    }
  end

  defp reason_code(%Error{kind: kind}), do: Atom.to_string(kind)
  defp reason_code({_operation, reason}), do: reason_code(reason)
end
