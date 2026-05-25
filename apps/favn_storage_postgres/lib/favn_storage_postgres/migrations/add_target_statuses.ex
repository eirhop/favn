defmodule FavnStoragePostgres.Migrations.AddTargetStatuses do
  @moduledoc false

  use Ecto.Migration

  def up do
    create_if_not_exists table(:favn_target_statuses, primary_key: false) do
      add(:manifest_version_id, :string, null: false)
      add(:target_kind, :string, null: false)
      add(:target_id, :string, null: false)
      add(:target_ref_text, :string, null: false)
      add(:status, :string, null: false)
      add(:latest_run_id, :string)
      add(:latest_run_status, :string)
      add(:latest_run_at, :utc_datetime_usec)
      add(:latest_success_run_id, :string)
      add(:latest_success_at, :utc_datetime_usec)
      add(:latest_failure_run_id, :string)
      add(:latest_failure_at, :utc_datetime_usec)
      add(:in_flight_run_id, :string)
      add(:freshness_status, :string)
      add(:freshness_key, :string)
      add(:updated_at, :utc_datetime_usec, null: false)
      add(:updated_seq, :bigint, null: false, default: 0)
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

  def down do
    drop_if_exists(table(:favn_target_statuses))
  end
end
