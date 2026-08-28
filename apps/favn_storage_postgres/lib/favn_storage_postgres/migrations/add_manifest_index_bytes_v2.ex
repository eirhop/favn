defmodule FavnStoragePostgres.Migrations.AddManifestIndexBytesV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:manifest_versions, prefix: @prefix) do
      add(:manifest_index_bytes, :bigint)
    end

    execute("""
    UPDATE #{@prefix}.manifest_versions
    SET manifest_index_bytes = octet_length(manifest::text)
    """)

    execute("""
    ALTER TABLE #{@prefix}.manifest_versions
    ALTER COLUMN manifest_index_bytes SET NOT NULL
    """)

    create(
      constraint(:manifest_versions, :manifest_versions_index_bytes_valid,
        prefix: @prefix,
        check: "manifest_index_bytes >= 0"
      )
    )
  end

  def down do
    drop(
      constraint(:manifest_versions, :manifest_versions_index_bytes_valid, prefix: @prefix)
    )

    alter table(:manifest_versions, prefix: @prefix) do
      remove(:manifest_index_bytes)
    end
  end
end
