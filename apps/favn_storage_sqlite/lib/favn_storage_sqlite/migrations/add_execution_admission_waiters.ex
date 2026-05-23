defmodule FavnStorageSqlite.Migrations.AddExecutionAdmissionWaiters do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_execution_admission_waiters, primary_key: false) do
      add(:waiter_id, :string, primary_key: true)
      add(:run_id, :string, null: false)
      add(:asset_step_id, :string, null: false)
      add(:queue_reason, :string, null: false)
      add(:blocked_scope_kind, :string, null: false)
      add(:blocked_scope_key, :string, null: false)
      add(:inserted_at, :text, null: false)
      add(:updated_at, :text, null: false)
      add(:deadline_at, :text)
      add(:wake_generation, :integer, null: false)
      add(:waiter_payload, :text, null: false)
    end

    create_if_not_exists(index(:favn_execution_admission_waiters, [:run_id]))
    create_if_not_exists(index(:favn_execution_admission_waiters, [:deadline_at]))

    create_if_not_exists(
      index(:favn_execution_admission_waiters, [
        :blocked_scope_kind,
        :blocked_scope_key,
        :inserted_at,
        :waiter_id
      ])
    )
  end
end
