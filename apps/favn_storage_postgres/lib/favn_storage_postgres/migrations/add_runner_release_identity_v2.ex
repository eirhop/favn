defmodule FavnStoragePostgres.Migrations.AddRunnerReleaseIdentityV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @runner_bound_schema_version 10

  def up do
    alter table(:manifest_versions, prefix: @prefix) do
      add(:required_runner_release_id, :string, null: true)
    end

    execute("""
    UPDATE #{@prefix}.manifest_versions
    SET required_runner_release_id = manifest ->> 'required_runner_release_id'
    WHERE schema_version >= #{@runner_bound_schema_version}
    """)

    execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM #{@prefix}.manifest_versions
        WHERE schema_version >= #{@runner_bound_schema_version}
          AND (
            required_runner_release_id IS NULL
            OR required_runner_release_id !~ '^rr_[0-9a-f]{64}$'
          )
      ) THEN
        RAISE EXCEPTION
          'current manifest rows cannot be bound to a valid runner release identity'
          USING ERRCODE = '23514',
                HINT = 'republish invalid current manifests before retrying the migration';
      END IF;
    END
    $$
    """)

    create(
      constraint(:manifest_versions, :manifest_versions_runner_release_valid,
        prefix: @prefix,
        check: """
        (schema_version < #{@runner_bound_schema_version} AND required_runner_release_id IS NULL)
        OR
        (schema_version >= #{@runner_bound_schema_version}
         AND required_runner_release_id IS NOT NULL
         AND required_runner_release_id ~ '^rr_[0-9a-f]{64}$')
        """
      )
    )
  end

  def down do
    drop(constraint(:manifest_versions, :manifest_versions_runner_release_valid, prefix: @prefix))

    alter table(:manifest_versions, prefix: @prefix) do
      remove(:required_runner_release_id)
    end
  end
end
