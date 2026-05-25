defmodule FavnOrchestrator.ScheduleListEntry do
  @moduledoc """
  Bounded operator-facing schedule list read model.

  The struct separates operator activation from runtime activity so thin UI
  clients do not infer scheduler semantics from cursor fields.
  """

  alias FavnOrchestrator.SchedulerEntry

  @type activation_state :: :pending_activation | :enabled | :disabled | :needs_review | :retired
  @type runtime_state :: :inactive | :idle | :running | :queued

  @type t :: %__MODULE__{
          id: String.t(),
          pipeline_module: module(),
          schedule_id: atom() | nil,
          cron: String.t() | nil,
          timezone: String.t() | nil,
          overlap: atom() | nil,
          missed: atom() | nil,
          manifest_active?: boolean(),
          activation_state: activation_state(),
          effective_enabled?: boolean(),
          runtime_state: runtime_state(),
          window: term(),
          schedule_fingerprint: String.t() | nil,
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          last_evaluated_at: DateTime.t() | nil,
          last_due_at: DateTime.t() | nil,
          next_due_at: DateTime.t() | nil,
          last_submitted_due_at: DateTime.t() | nil,
          in_flight_run_id: String.t() | nil,
          queued_due_at: DateTime.t() | nil,
          last_scheduler_error: FavnOrchestrator.SchedulerError.t() | nil,
          updated_at: DateTime.t() | nil
        }

  @enforce_keys [:id, :pipeline_module, :activation_state, :effective_enabled?, :runtime_state]
  defstruct [
    :id,
    :pipeline_module,
    :schedule_id,
    :cron,
    :timezone,
    :overlap,
    :missed,
    :window,
    :schedule_fingerprint,
    :manifest_version_id,
    :manifest_content_hash,
    :last_evaluated_at,
    :last_due_at,
    :next_due_at,
    :last_submitted_due_at,
    :in_flight_run_id,
    :queued_due_at,
    :last_scheduler_error,
    :updated_at,
    manifest_active?: false,
    activation_state: :pending_activation,
    effective_enabled?: false,
    runtime_state: :inactive
  ]

  @doc """
  Builds a schedule list read model from the stable scheduler inspection entry.
  """
  @spec from_scheduler_entry(String.t(), SchedulerEntry.t()) :: t()
  def from_scheduler_entry(id, %SchedulerEntry{} = entry) when is_binary(id) do
    %__MODULE__{
      id: id,
      pipeline_module: entry.pipeline_module,
      schedule_id: entry.schedule_id,
      cron: entry.cron,
      timezone: entry.timezone,
      overlap: entry.overlap,
      missed: entry.missed,
      manifest_active?: entry.active,
      activation_state: entry.activation_state,
      effective_enabled?: entry.effective_enabled?,
      runtime_state: entry.runtime_state,
      window: entry.window,
      schedule_fingerprint: entry.schedule_fingerprint,
      manifest_version_id: entry.manifest_version_id,
      manifest_content_hash: entry.manifest_content_hash,
      last_evaluated_at: entry.last_evaluated_at,
      last_due_at: entry.last_due_at,
      next_due_at: entry.next_due_at,
      last_submitted_due_at: entry.last_submitted_due_at,
      in_flight_run_id: entry.in_flight_run_id,
      queued_due_at: entry.queued_due_at,
      last_scheduler_error: entry.last_scheduler_error,
      updated_at: entry.updated_at
    }
  end
end
