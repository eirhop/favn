defmodule Favn.Scheduler.State do
  @moduledoc """
  Persisted runtime scheduler state for one scheduled pipeline stream.
  """

  @type t :: %__MODULE__{
          pipeline_module: module(),
          schedule_id: atom() | nil,
          schedule_fingerprint: String.t() | nil,
          last_evaluated_at: DateTime.t() | nil,
          last_due_at: DateTime.t() | nil,
          last_submitted_due_at: DateTime.t() | nil,
          in_flight_run_id: String.t() | nil,
          queued_due_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  defstruct pipeline_module: nil,
            schedule_id: nil,
            schedule_fingerprint: nil,
            last_evaluated_at: nil,
            last_due_at: nil,
            last_submitted_due_at: nil,
            in_flight_run_id: nil,
            queued_due_at: nil,
            updated_at: nil
end
