defmodule FavnStorageSqlite.Migrations.AddRuntimeInputPins do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_runtime_input_pins, primary_key: false) do
      add(:run_id, :string, null: false)
      add(:node_key_hash, :string, null: false)
      add(:payload_fingerprint, :string, null: false)
      add(:record_payload, :text, null: false)
      add(:inserted_at, :text, null: false)
      add(:updated_at, :text, null: false)
    end

    create_if_not_exists(unique_index(:favn_runtime_input_pins, [:run_id, :node_key_hash]))

    create_if_not_exists(index(:favn_runtime_input_pins, [:run_id]))
  end
end
