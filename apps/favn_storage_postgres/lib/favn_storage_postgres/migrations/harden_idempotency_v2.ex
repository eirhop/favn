defmodule FavnStoragePostgres.Migrations.HardenIdempotencyV2 do
  @moduledoc false
  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:idempotency_records, prefix: @prefix) do
      add(:reservation_generation, :bigint, null: false, default: 1)
      add(:resource_kind, :text)
      add(:resource_id, :text)
    end

    create(
      constraint(:idempotency_records, :idempotency_records_payload_bounded,
        prefix: @prefix,
        check:
          "octet_length(operation) BETWEEN 1 AND 128 AND " <>
            "octet_length(principal_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(key_hash) BETWEEN 16 AND 64 AND " <>
            "octet_length(request_fingerprint) BETWEEN 16 AND 64 AND " <>
            "reservation_generation > 0 AND " <>
            "(response IS NULL OR pg_column_size(response) <= 65536) AND " <>
            "(response_status IS NULL OR response_status BETWEEN 100 AND 599) AND " <>
            "(resource_kind IS NULL OR octet_length(resource_kind) BETWEEN 1 AND 64) AND " <>
            "(resource_id IS NULL OR octet_length(resource_id) BETWEEN 1 AND 512)"
      )
    )
  end

  def down do
    drop(constraint(:idempotency_records, :idempotency_records_payload_bounded, prefix: @prefix))

    alter table(:idempotency_records, prefix: @prefix) do
      remove(:resource_id)
      remove(:resource_kind)
      remove(:reservation_generation)
    end
  end
end
