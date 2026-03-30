defmodule Favn.Runtime.StepState do
  @moduledoc """
  Internal coordinator-owned step state for one planned asset ref.
  """

  alias Favn.Ref

  @type status ::
          :pending | :ready | :running | :success | :failed | :cancelled | :skipped | :timed_out

  @type t :: %__MODULE__{
          ref: Ref.t(),
          stage: non_neg_integer(),
          upstream: [Ref.t()],
          downstream: [Ref.t()],
          status: status(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          output: term() | nil,
          meta: map(),
          error: map() | nil,
          terminal_reason: term() | nil
        }

  defstruct [
    :ref,
    :stage,
    upstream: [],
    downstream: [],
    status: :pending,
    started_at: nil,
    finished_at: nil,
    duration_ms: nil,
    output: nil,
    meta: %{},
    error: nil,
    terminal_reason: nil
  ]
end
