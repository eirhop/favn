defmodule Favn.Runtime.State do
  @moduledoc """
  Internal run-scoped runtime state owned by the coordinator.
  """

  alias Favn.Plan
  alias Favn.Ref
  alias Favn.Runtime.StepState

  @type run_status ::
          :pending | :running | :cancelling | :cancelled | :success | :failed | :timed_out

  @type t :: %__MODULE__{
          run_id: Favn.run_id(),
          run_status: run_status(),
          target_refs: [Ref.t()],
          plan: Plan.t(),
          params: map(),
          event_seq: non_neg_integer(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          cancel_requested_at: DateTime.t() | nil,
          deadline_at: DateTime.t() | nil,
          steps: %{Ref.t() => StepState.t()},
          ready_queue: [Ref.t()],
          running_steps: MapSet.t(Ref.t()),
          completed_steps: MapSet.t(Ref.t()),
          outputs: %{Ref.t() => term()},
          run_error: term() | nil
        }

  defstruct [
    :run_id,
    :target_refs,
    :plan,
    run_status: :pending,
    params: %{},
    event_seq: 0,
    started_at: nil,
    finished_at: nil,
    cancel_requested_at: nil,
    deadline_at: nil,
    steps: %{},
    ready_queue: [],
    running_steps: MapSet.new(),
    completed_steps: MapSet.new(),
    outputs: %{},
    run_error: nil
  ]
end
