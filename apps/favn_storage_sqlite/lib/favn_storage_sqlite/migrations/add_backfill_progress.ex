defmodule FavnStorageSqlite.Migrations.AddBackfillProgress do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_backfill_progress, primary_key: false) do
      add(:backfill_run_id, :string, primary_key: true)
      add(:total_count, :integer, null: false)
      add(:pending_count, :integer, null: false)
      add(:running_count, :integer, null: false)
      add(:ok_count, :integer, null: false)
      add(:partial_count, :integer, null: false)
      add(:error_count, :integer, null: false)
      add(:cancelled_count, :integer, null: false)
      add(:timed_out_count, :integer, null: false)
      add(:status, :string, null: false)
      add(:record_payload, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create_if_not_exists(index(:favn_backfill_progress, [:status, :updated_at]))
    create_if_not_exists(index(:favn_backfill_windows, [:backfill_run_id, :status]))
  end
end
