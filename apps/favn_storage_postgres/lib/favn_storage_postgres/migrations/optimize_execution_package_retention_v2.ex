defmodule FavnStoragePostgres.Migrations.OptimizeExecutionPackageRetentionV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:execution_packages, prefix: @prefix) do
      add(:first_linked_at, :timestamptz)
    end

    execute("""
    UPDATE #{@prefix}.execution_packages AS package
    SET first_linked_at = package.inserted_at
    WHERE EXISTS (
      SELECT 1
      FROM #{@prefix}.manifest_execution_packages AS manifest_package
      WHERE manifest_package.package_hash = package.content_hash
    )
    """)

    create(
      index(:execution_packages, [:inserted_at, :content_hash],
        prefix: @prefix,
        name: :execution_packages_unlinked_retention_idx,
        where: "first_linked_at IS NULL"
      )
    )
  end

  def down do
    drop(
      index(:execution_packages, [],
        prefix: @prefix,
        name: :execution_packages_unlinked_retention_idx
      )
    )

    alter table(:execution_packages, prefix: @prefix) do
      remove(:first_linked_at)
    end
  end
end
