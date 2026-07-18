defmodule FavnRunner.ExecutionLifecycle.Execution do
  @moduledoc false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork

  @type status :: :running | :completed

  @type t :: %__MODULE__{
          id: String.t(),
          work: RunnerWork.t() | nil,
          work_fingerprint: binary(),
          status: status(),
          pid: pid() | nil,
          monitor_ref: reference() | nil,
          result: RunnerResult.t() | nil,
          events: [term()],
          logs: [term()],
          started_at: DateTime.t() | nil,
          completed_at: DateTime.t() | nil,
          dropped_event_count: non_neg_integer(),
          dropped_log_count: non_neg_integer(),
          retained_bytes: non_neg_integer(),
          result_truncated: boolean()
        }

  defstruct id: nil,
            work: nil,
            work_fingerprint: nil,
            status: :running,
            pid: nil,
            monitor_ref: nil,
            result: nil,
            events: [],
            logs: [],
            started_at: nil,
            completed_at: nil,
            dropped_event_count: 0,
            dropped_log_count: 0,
            retained_bytes: 0,
            result_truncated: false

  @spec running(String.t(), RunnerWork.t(), pid(), reference(), DateTime.t()) :: t()
  def running(id, %RunnerWork{} = work, pid, monitor_ref, %DateTime{} = started_at)
      when is_binary(id) and is_pid(pid) and is_reference(monitor_ref) do
    %__MODULE__{
      id: id,
      work: work,
      work_fingerprint: RunnerWork.replay_fingerprint(work),
      status: :running,
      pid: pid,
      monitor_ref: monitor_ref,
      started_at: started_at
    }
  end

  @spec completed(
          String.t(),
          RunnerWork.t(),
          RunnerResult.t(),
          DateTime.t(),
          non_neg_integer(),
          boolean()
        ) :: t()
  def completed(
        id,
        %RunnerWork{} = work,
        %RunnerResult{} = result,
        %DateTime{} = completed_at,
        retained_bytes,
        truncated?
      )
      when is_binary(id) do
    %__MODULE__{
      id: id,
      work: nil,
      work_fingerprint: RunnerWork.replay_fingerprint(work),
      status: :completed,
      result: result,
      completed_at: completed_at,
      retained_bytes: retained_bytes,
      result_truncated: truncated?
    }
    |> with_retained_size()
  end

  @spec complete(t(), RunnerResult.t(), DateTime.t(), non_neg_integer(), boolean()) :: t()
  def complete(
        %__MODULE__{} = execution,
        %RunnerResult{} = result,
        %DateTime{} = completed_at,
        retained_bytes,
        truncated?
      ) do
    fingerprint = execution.work_fingerprint || RunnerWork.replay_fingerprint(execution.work)

    %{
      execution
      | status: :completed,
        work: nil,
        work_fingerprint: fingerprint,
        pid: nil,
        monitor_ref: nil,
        result: result,
        completed_at: completed_at,
        retained_bytes: retained_bytes,
        result_truncated: truncated?
    }
    |> with_retained_size()
  end

  @doc false
  @spec with_retained_size(t()) :: t()
  def with_retained_size(%__MODULE__{} = execution) do
    %{execution | retained_bytes: :erlang.external_size(%{execution | retained_bytes: 0})}
  end

  @doc false
  @spec bound_retained(t(), non_neg_integer()) :: {t(), non_neg_integer(), non_neg_integer()}
  def bound_retained(%__MODULE__{} = execution, max_bytes)
      when is_integer(max_bytes) and max_bytes >= 0 do
    do_bound_retained(with_retained_size(execution), max_bytes, 0, 0)
  end

  defp do_bound_retained(execution, max_bytes, dropped_logs, dropped_events)
       when execution.retained_bytes > max_bytes and execution.logs != [] do
    execution
    |> Map.update!(:logs, &Enum.drop(&1, -1))
    |> Map.update!(:dropped_log_count, &(&1 + 1))
    |> with_retained_size()
    |> do_bound_retained(max_bytes, dropped_logs + 1, dropped_events)
  end

  defp do_bound_retained(execution, max_bytes, dropped_logs, dropped_events)
       when execution.retained_bytes > max_bytes and execution.events != [] do
    execution
    |> Map.update!(:events, &Enum.drop(&1, -1))
    |> Map.update!(:dropped_event_count, &(&1 + 1))
    |> with_retained_size()
    |> do_bound_retained(max_bytes, dropped_logs, dropped_events + 1)
  end

  defp do_bound_retained(execution, _max_bytes, dropped_logs, dropped_events),
    do: {execution, dropped_logs, dropped_events}
end
