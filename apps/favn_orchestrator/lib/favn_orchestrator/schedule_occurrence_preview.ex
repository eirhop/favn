defmodule FavnOrchestrator.ScheduleOccurrencePreview do
  @moduledoc """
  Operator-facing preview of one future schedule occurrence.

  Preview rows are computed in the orchestrator so UI clients do not duplicate
  cron, activation, overlap, missed, or window semantics.
  """

  @type status :: :upcoming | :queued | :running | :blocked | :disabled

  @type t :: %__MODULE__{
          schedule_entry_id: String.t(),
          due_at: DateTime.t(),
          timezone: String.t(),
          window: map() | nil,
          status: status(),
          notes: [String.t()]
        }

  @enforce_keys [:schedule_entry_id, :due_at, :timezone, :status]
  defstruct [:schedule_entry_id, :due_at, :timezone, :window, status: :upcoming, notes: []]
end
