defmodule FavnStorageSqlite.Migrations.AddAssetFreshnessState do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_asset_freshness_states, primary_key: false) do
      add(:asset_ref_module, :string, null: false)
      add(:asset_ref_name, :string, null: false)
      add(:freshness_key, :string, null: false)
      add(:status, :string, null: false)
      add(:freshness_version, :string)
      add(:latest_success_run_id, :string)
      add(:latest_attempt_run_id, :string)
      add(:latest_attempt_status, :string)
      add(:manifest_version_id, :string)
      add(:manifest_content_hash, :string)
      add(:record_payload, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create_if_not_exists(
      unique_index(:favn_asset_freshness_states, [
        :asset_ref_module,
        :asset_ref_name,
        :freshness_key
      ])
    )

    create_if_not_exists(index(:favn_asset_freshness_states, [:status, :updated_at]))
    create_if_not_exists(index(:favn_asset_freshness_states, [:manifest_version_id]))
  end
end
