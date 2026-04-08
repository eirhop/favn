defmodule Favn.Storage.SQLite.Migrations.CreateSchedulerStates do
  use Ecto.Migration

  def change do
    create table(:scheduler_states, primary_key: false) do
      add(:pipeline_module, :text, primary_key: true)
      add(:schedule_id, :text)
      add(:schedule_fingerprint, :text)
      add(:last_evaluated_at, :utc_datetime)
      add(:last_due_at, :utc_datetime)
      add(:last_submitted_due_at, :utc_datetime)
      add(:in_flight_run_id, :text)
      add(:queued_due_at, :utc_datetime)
      add(:updated_at, :utc_datetime)
    end
  end
end
