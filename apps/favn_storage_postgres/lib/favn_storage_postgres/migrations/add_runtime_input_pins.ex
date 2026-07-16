defmodule FavnStoragePostgres.Migrations.AddRuntimeInputPins do
  @moduledoc false

  use Ecto.Migration

  def up do
    create_if_not_exists table(:favn_runtime_input_pins, primary_key: false) do
      add(:run_id, :string, null: false)
      add(:node_key_hash, :string, null: false)
      add(:payload_fingerprint, :string, null: false)
      add(:record_payload, :text, null: false)
      add(:inserted_at, :utc_datetime_usec, null: false)
      add(:updated_at, :utc_datetime_usec, null: false)
    end

    create_if_not_exists(unique_index(:favn_runtime_input_pins, [:run_id, :node_key_hash]))

    create_if_not_exists(index(:favn_runtime_input_pins, [:run_id]))
  end

  def down, do: drop_if_exists(table(:favn_runtime_input_pins))
end
