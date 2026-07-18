defmodule FavnStoragePostgres.Migrations.AddRuntimeInputKeyInventoryV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:runtime_input_key_versions, prefix: @prefix, primary_key: false) do
      add(:key_version, :integer, null: false, primary_key: true)
      add(:first_used_at, :timestamptz, null: false)
    end

    create(
      constraint(:runtime_input_key_versions, :runtime_input_key_versions_key_version_valid,
        prefix: @prefix,
        check: "key_version > 0"
      )
    )

    create(
      index(:runtime_input_pins, [:encryption_key_version],
        prefix: @prefix,
        name: :runtime_input_pins_key_version_idx
      )
    )

    execute("""
    INSERT INTO #{@prefix}.runtime_input_key_versions (key_version, first_used_at)
    SELECT encryption_key_version, min(inserted_at)
    FROM #{@prefix}.runtime_input_pins
    GROUP BY encryption_key_version
    ON CONFLICT (key_version) DO NOTHING
    """)
  end

  def down do
    drop(
      index(:runtime_input_pins, [:encryption_key_version],
        prefix: @prefix,
        name: :runtime_input_pins_key_version_idx
      )
    )

    drop(table(:runtime_input_key_versions, prefix: @prefix))
  end
end
