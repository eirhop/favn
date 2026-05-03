defmodule FavnStorageSqlite.Migrations.AddIdempotencyRecords do
  @moduledoc false

  use Ecto.Migration

  def change do
    create_if_not_exists table(:favn_idempotency_records, primary_key: false) do
      add(:idempotency_record_id, :string, primary_key: true)
      add(:operation, :string, null: false)
      add(:idempotency_key_hash, :string, null: false)
      add(:actor_id, :string, null: false)
      add(:session_id, :string, null: false)
      add(:service_identity, :string, null: false)
      add(:request_fingerprint, :string, null: false)
      add(:status, :string, null: false)
      add(:response_status, :integer)
      add(:response_body_blob, :binary)
      add(:resource_type, :string)
      add(:resource_id, :string)
      add(:created_at, :text, null: false)
      add(:updated_at, :text, null: false)
      add(:expires_at, :text, null: false)
      add(:completed_at, :text)
    end

    create_if_not_exists(
      unique_index(:favn_idempotency_records, [
        :operation,
        :actor_id,
        :session_id,
        :service_identity,
        :idempotency_key_hash
      ])
    )

    create_if_not_exists(index(:favn_idempotency_records, [:expires_at]))
    create_if_not_exists(index(:favn_idempotency_records, [:resource_type, :resource_id]))
  end
end
