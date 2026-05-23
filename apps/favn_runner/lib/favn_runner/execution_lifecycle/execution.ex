defmodule FavnRunner.ExecutionLifecycle.Execution do
  @moduledoc false

  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork

  @type status :: :running | :completed

  @type t :: %__MODULE__{
          id: String.t(),
          work: RunnerWork.t(),
          status: status(),
          pid: pid() | nil,
          monitor_ref: reference() | nil,
          result: RunnerResult.t() | nil,
          events: [term()],
          logs: [term()],
          started_at: DateTime.t(),
          completed_at: DateTime.t() | nil,
          dropped_event_count: non_neg_integer(),
          dropped_log_count: non_neg_integer()
        }

  defstruct id: nil,
            work: nil,
            status: :running,
            pid: nil,
            monitor_ref: nil,
            result: nil,
            events: [],
            logs: [],
            started_at: nil,
            completed_at: nil,
            dropped_event_count: 0,
            dropped_log_count: 0

  @spec running(String.t(), RunnerWork.t(), pid(), reference(), DateTime.t()) :: t()
  def running(id, %RunnerWork{} = work, pid, monitor_ref, %DateTime{} = started_at)
      when is_binary(id) and is_pid(pid) and is_reference(monitor_ref) do
    %__MODULE__{
      id: id,
      work: work,
      status: :running,
      pid: pid,
      monitor_ref: monitor_ref,
      started_at: started_at
    }
  end

  @spec completed(String.t(), RunnerWork.t(), RunnerResult.t(), DateTime.t()) :: t()
  def completed(id, %RunnerWork{} = work, %RunnerResult{} = result, %DateTime{} = completed_at)
      when is_binary(id) do
    %__MODULE__{
      id: id,
      work: work,
      status: :completed,
      result: result,
      started_at: completed_at,
      completed_at: completed_at
    }
  end

  @spec complete(t(), RunnerResult.t(), DateTime.t()) :: t()
  def complete(%__MODULE__{} = execution, %RunnerResult{} = result, %DateTime{} = completed_at) do
    %{
      execution
      | status: :completed,
        pid: nil,
        monitor_ref: nil,
        result: result,
        completed_at: completed_at
    }
  end
end
