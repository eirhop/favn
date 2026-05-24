defmodule FavnOrchestrator.SchedulerEntry do
  @moduledoc """
  Stable operator-facing scheduler inspection entry.
  """

  alias Favn.Manifest.Schedule
  alias Favn.Scheduler.State
  alias Favn.Window.Policy

  @type t :: %__MODULE__{
          pipeline_module: module(),
          schedule_id: atom() | nil,
          cron: String.t() | nil,
          timezone: String.t() | nil,
          overlap: atom() | nil,
          missed: atom() | nil,
          active: boolean(),
          activation_state: atom(),
          effective_enabled?: boolean(),
          runtime_state: :inactive | :idle | :running | :queued,
          window: Policy.t() | nil,
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

  defstruct [
    :pipeline_module,
    :schedule_id,
    :cron,
    :timezone,
    :overlap,
    :missed,
    :window,
    :activation_state,
    :effective_enabled?,
    :runtime_state,
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
    active: false
  ]

  @spec from_runtime(map(), State.t() | map() | nil) :: t()
  def from_runtime(entry, scheduler_state \\ nil) when is_map(entry) do
    schedule = Map.get(entry, :schedule)
    state = normalize_state(entry, scheduler_state)

    active = schedule_active(schedule)
    activation_state = Map.get(state, :activation_state, :pending_activation)
    effective_enabled? = active and activation_state == :enabled

    %__MODULE__{
      pipeline_module: Map.get(entry, :module),
      schedule_id: schedule_id(schedule),
      cron: schedule_field(schedule, :cron),
      timezone: schedule_field(schedule, :timezone),
      overlap: schedule_field(schedule, :overlap),
      missed: schedule_field(schedule, :missed),
      active: active,
      activation_state: activation_state,
      effective_enabled?: effective_enabled?,
      runtime_state: runtime_state(effective_enabled?, state),
      window: Map.get(entry, :window),
      schedule_fingerprint: Map.get(entry, :schedule_fingerprint),
      manifest_version_id: Map.get(entry, :manifest_version_id),
      manifest_content_hash: Map.get(entry, :manifest_content_hash),
      last_evaluated_at: Map.get(state, :last_evaluated_at),
      last_due_at: Map.get(state, :last_due_at),
      next_due_at: Map.get(entry, :next_due_at),
      last_submitted_due_at: Map.get(state, :last_submitted_due_at),
      in_flight_run_id: Map.get(state, :in_flight_run_id),
      queued_due_at: Map.get(state, :queued_due_at),
      last_scheduler_error: Map.get(state, :last_scheduler_error),
      updated_at: Map.get(state, :updated_at)
    }
  end

  defp normalize_state(entry, nil) do
    %State{
      pipeline_module: Map.get(entry, :module),
      schedule_id: schedule_id(Map.get(entry, :schedule)),
      schedule_fingerprint: Map.get(entry, :schedule_fingerprint),
      activation_state: :pending_activation
    }
  end

  defp normalize_state(_entry, %State{} = state), do: state
  defp normalize_state(_entry, state) when is_map(state), do: struct(State, state)

  defp schedule_id(%Schedule{name: name}), do: name
  defp schedule_id(%{name: name}), do: name
  defp schedule_id(_schedule), do: nil

  defp schedule_field(%Schedule{} = schedule, field), do: Map.get(schedule, field)
  defp schedule_field(%{} = schedule, field), do: Map.get(schedule, field)
  defp schedule_field(_schedule, _field), do: nil

  defp schedule_active(%Schedule{active: active}), do: active == true
  defp schedule_active(%{active: active}), do: active == true
  defp schedule_active(_schedule), do: false

  defp runtime_state(false, _state), do: :inactive
  defp runtime_state(true, %{in_flight_run_id: run_id}) when is_binary(run_id), do: :running
  defp runtime_state(true, %{queued_due_at: %DateTime{}}), do: :queued
  defp runtime_state(true, _state), do: :idle
end
