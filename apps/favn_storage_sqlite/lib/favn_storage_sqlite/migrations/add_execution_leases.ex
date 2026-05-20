defmodule FavnStorageSqlite.Migrations.AddExecutionLeases do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_execution_leases, primary_key: false) do
      add(:lease_id, :string, primary_key: true)
      add(:run_id, :string, null: false)
      add(:asset_step_id, :string, null: false)
      add(:acquired_at, :text, null: false)
      add(:expires_at, :text, null: false)
      add(:lease_payload, :text, null: false)
    end

    create_if_not_exists table(:favn_execution_lease_scopes, primary_key: false) do
      add(:lease_id, :string, null: false)
      add(:scope_kind, :string, null: false)
      add(:scope_key, :string, null: false)
      add(:scope_limit, :integer, null: false)
    end

    create_if_not_exists(index(:favn_execution_leases, [:expires_at]))
    create_if_not_exists(index(:favn_execution_leases, [:run_id]))
    create_if_not_exists(index(:favn_execution_lease_scopes, [:scope_kind, :scope_key]))

    create_if_not_exists(
      unique_index(:favn_execution_lease_scopes, [:lease_id, :scope_kind, :scope_key])
    )
  end
end
