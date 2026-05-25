defmodule FavnStorageSqlite.Migrations.AddTargetStatuses do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_target_statuses, primary_key: false) do
      add(:manifest_version_id, :string, null: false)
      add(:target_kind, :string, null: false)
      add(:target_id, :string, null: false)
      add(:target_ref_text, :string, null: false)
      add(:status, :string, null: false)
      add(:latest_run_id, :string)
      add(:latest_run_status, :string)
      add(:latest_run_at, :text)
      add(:latest_success_run_id, :string)
      add(:latest_success_at, :text)
      add(:latest_failure_run_id, :string)
      add(:latest_failure_at, :text)
      add(:in_flight_run_id, :string)
      add(:freshness_status, :string)
      add(:freshness_key, :string)
      add(:updated_at, :text, null: false)
      add(:updated_seq, :integer, null: false, default: 0)
      add(:record_payload, :text, null: false)
    end

    create_if_not_exists(
      unique_index(:favn_target_statuses, [:manifest_version_id, :target_kind, :target_id])
    )

    create_if_not_exists(index(:favn_target_statuses, [:manifest_version_id, :target_kind]))

    create_if_not_exists(
      index(:favn_target_statuses, [:manifest_version_id, :target_kind, :status, :updated_at])
    )

    create_if_not_exists(index(:favn_target_statuses, [:latest_run_id]))
    create_if_not_exists(index(:favn_target_statuses, [:in_flight_run_id]))
  end
end
