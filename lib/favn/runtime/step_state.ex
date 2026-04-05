defmodule Favn.Runtime.StepState do
  @moduledoc """
  Internal coordinator-owned step state for one planned asset ref.
  """

  alias Favn.Ref

  @type status ::
          :pending
          | :ready
          | :running
          | :retrying
          | :success
          | :failed
          | :cancelled
          | :skipped
          | :timed_out

  @type attempt_result_status :: :ok | :error | :cancelled | :timed_out

  @type attempt_result :: %{
          attempt: pos_integer(),
          exec_ref: reference() | nil,
          started_at: DateTime.t(),
          finished_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          status: attempt_result_status(),
          meta: map(),
          error: map() | nil
        }

  @type t :: %__MODULE__{
          ref: Ref.t(),
          stage: non_neg_integer(),
          upstream: [Ref.t()],
          downstream: [Ref.t()],
          status: status(),
          attempt: non_neg_integer(),
          max_attempts: pos_integer(),
          next_retry_at: DateTime.t() | nil,
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          meta: map(),
          error: map() | nil,
          attempts: [attempt_result()],
          terminal_reason: term() | nil
        }

  defstruct [
    :ref,
    :stage,
    upstream: [],
    downstream: [],
    status: :pending,
    attempt: 0,
    max_attempts: 1,
    next_retry_at: nil,
    started_at: nil,
    finished_at: nil,
    duration_ms: nil,
    meta: %{},
    error: nil,
    attempts: [],
    terminal_reason: nil
  ]
end
