defmodule FavnStoragePostgres.Migrations.EnforceRunPlanManifestIdentityV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      unique_index(:runs, [:workspace_id, :run_id, :manifest_version_id],
        prefix: @prefix,
        name: :runs_plan_manifest_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.run_plans
      DROP CONSTRAINT run_plans_run_fk,
      ADD CONSTRAINT run_plans_run_manifest_fk
        FOREIGN KEY (workspace_id, run_id, manifest_version_id)
        REFERENCES #{@prefix}.runs(workspace_id, run_id, manifest_version_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    """)
  end

  def down do
    execute("""
    ALTER TABLE #{@prefix}.run_plans
      DROP CONSTRAINT run_plans_run_manifest_fk,
      ADD CONSTRAINT run_plans_run_fk
        FOREIGN KEY (workspace_id, run_id)
        REFERENCES #{@prefix}.runs(workspace_id, run_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
    """)

    drop(
      index(:runs, [],
        prefix: @prefix,
        name: :runs_plan_manifest_uidx
      )
    )
  end
end
