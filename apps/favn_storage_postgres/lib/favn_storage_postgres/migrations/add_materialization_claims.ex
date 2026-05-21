defmodule FavnStoragePostgres.Migrations.AddMaterializationClaims do
  @moduledoc false

  use Ecto.Migration

  def up do
    create_if_not_exists table(:favn_materialization_claims, primary_key: false) do
      add(:claim_key, :string, null: false, primary_key: true)
      add(:asset_ref_module, :string, null: false)
      add(:asset_ref_name, :string, null: false)
      add(:freshness_key, :string, null: false)
      add(:input_fingerprint, :string, null: false)
      add(:run_id, :string)
      add(:asset_step_id, :string)
      add(:node_key, :string)
      add(:runner_execution_id, :string)
      add(:manifest_version_id, :string)
      add(:manifest_content_hash, :string)
      add(:freshness_version, :string)
      add(:status, :string, null: false)
      add(:claimed_at, :utc_datetime_usec, null: false)
      add(:heartbeat_at, :utc_datetime_usec)
      add(:expires_at, :utc_datetime_usec, null: false)
      add(:finished_at, :utc_datetime_usec)
      add(:record_payload, :text, null: false)
    end

    create_if_not_exists(unique_index(:favn_materialization_claims, [:claim_key]))
    create_if_not_exists(index(:favn_materialization_claims, [:status, :expires_at]))
    create_if_not_exists(index(:favn_materialization_claims, [:run_id]))

    create_if_not_exists(
      index(:favn_materialization_claims, [
        :asset_ref_module,
        :asset_ref_name,
        :freshness_key
      ])
    )
  end

  def down do
    drop_if_exists(table(:favn_materialization_claims))
  end
end
