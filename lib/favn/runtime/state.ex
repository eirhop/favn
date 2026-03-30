defmodule Favn.Runtime.State do
  @moduledoc """
  Internal run-scoped runtime state owned by the coordinator.
  """

  alias Favn.Plan
  alias Favn.Ref
  alias Favn.Runtime.StepState

  @type run_status ::
          :pending
          | :running
          | :cancelling
          | :cancelled
          | :timing_out
          | :success
          | :failed
          | :timed_out

  @type exec_info :: %{ref: Ref.t(), monitor_ref: reference(), pid: pid()}

  @type t :: %__MODULE__{
          run_id: Favn.run_id(),
          run_status: run_status(),
          target_refs: [Ref.t()],
          plan: Plan.t(),
          params: map(),
          max_concurrency: pos_integer(),
          admission_open?: boolean(),
          event_seq: non_neg_integer(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          cancel_requested_at: DateTime.t() | nil,
          timeout_ms: pos_integer() | nil,
          deadline_at: DateTime.t() | nil,
          timeout_timer_ref: reference() | nil,
          steps: %{Ref.t() => StepState.t()},
          ready_queue: [Ref.t()],
          running_steps: MapSet.t(Ref.t()),
          completed_steps: MapSet.t(Ref.t()),
          inflight_execs: %{reference() => exec_info()},
          exec_refs_by_monitor: %{reference() => reference()},
          completed_exec_refs: MapSet.t(reference()),
          outputs: %{Ref.t() => term()},
          run_error: term() | nil,
          run_terminal_reason: map() | nil
        }

  defstruct [
    :run_id,
    :target_refs,
    :plan,
    run_status: :pending,
    params: %{},
    max_concurrency: 1,
    admission_open?: true,
    event_seq: 0,
    started_at: nil,
    finished_at: nil,
    cancel_requested_at: nil,
    timeout_ms: nil,
    deadline_at: nil,
    timeout_timer_ref: nil,
    steps: %{},
    ready_queue: [],
    running_steps: MapSet.new(),
    completed_steps: MapSet.new(),
    inflight_execs: %{},
    exec_refs_by_monitor: %{},
    completed_exec_refs: MapSet.new(),
    outputs: %{},
    run_error: nil,
    run_terminal_reason: nil
  ]
end
