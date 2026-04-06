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

  @type retry_class ::
          :exception | :exit | :throw | :timeout | :executor_error | :error_return

  @type retry_policy :: %{
          max_attempts: pos_integer(),
          delay_ms: non_neg_integer(),
          retry_on: [retry_class()]
        }

  @type t :: %__MODULE__{
          run_id: Favn.run_id(),
          run_status: run_status(),
          target_refs: [Ref.t()],
          plan: Plan.t(),
          params: map(),
          pipeline_context: map() | nil,
          max_concurrency: pos_integer(),
          admission_open?: boolean(),
          event_seq: non_neg_integer(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          cancel_requested_at: DateTime.t() | nil,
          timeout_ms: pos_integer() | nil,
          submit_kind: :asset | :pipeline | :rerun,
          submit_ref: term() | nil,
          replay_mode: :exact,
          rerun_of_run_id: Favn.run_id() | nil,
          parent_run_id: Favn.run_id() | nil,
          root_run_id: Favn.run_id() | nil,
          lineage_depth: non_neg_integer(),
          operator_reason: term() | nil,
          deadline_at: DateTime.t() | nil,
          timeout_timer_ref: reference() | nil,
          retry_policy: retry_policy(),
          retry_timers: %{Ref.t() => reference()},
          steps: %{Ref.t() => StepState.t()},
          ready_queue: [Ref.t()],
          inflight_execs: %{reference() => exec_info()},
          exec_refs_by_monitor: %{reference() => reference()},
          completed_exec_refs: MapSet.t(reference()),
          run_error: term() | nil,
          run_terminal_reason: map() | nil
        }

  defstruct [
    :run_id,
    :target_refs,
    :plan,
    run_status: :pending,
    params: %{},
    pipeline_context: nil,
    max_concurrency: 1,
    admission_open?: true,
    event_seq: 0,
    started_at: nil,
    finished_at: nil,
    cancel_requested_at: nil,
    timeout_ms: nil,
    submit_kind: :asset,
    submit_ref: nil,
    replay_mode: :exact,
    rerun_of_run_id: nil,
    parent_run_id: nil,
    root_run_id: nil,
    lineage_depth: 0,
    operator_reason: nil,
    deadline_at: nil,
    timeout_timer_ref: nil,
    retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []},
    retry_timers: %{},
    steps: %{},
    ready_queue: [],
    inflight_execs: %{},
    exec_refs_by_monitor: %{},
    completed_exec_refs: MapSet.new(),
    run_error: nil,
    run_terminal_reason: nil
  ]
end
