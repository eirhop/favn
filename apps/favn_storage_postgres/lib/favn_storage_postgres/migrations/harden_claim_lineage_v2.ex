defmodule FavnStoragePostgres.Migrations.HardenClaimLineageV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create(
      unique_index(
        :run_targets,
        [:workspace_id, :run_id, :deployment_id, :target_kind, :target_id],
        prefix: @prefix,
        name: :run_targets_claim_lineage_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    ADD CONSTRAINT materialization_claims_run_target_fk
    FOREIGN KEY (workspace_id, run_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.run_targets(
      workspace_id, run_id, deployment_id, target_kind, target_id
    )
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.materializations
    ADD CONSTRAINT materializations_run_target_fk
    FOREIGN KEY (workspace_id, run_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.run_targets(
      workspace_id, run_id, deployment_id, target_kind, target_id
    )
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.schedule_cursors
    ADD CONSTRAINT schedule_cursors_claim_shape_v2
    CHECK (
      (claim_owner IS NULL AND claim_command_id IS NULL AND claim_expires_at IS NULL)
      OR
      (claim_owner IS NOT NULL AND claim_command_id IS NOT NULL
       AND claim_expires_at IS NOT NULL AND claim_generation > 0)
    )
    """)

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
    ADD CONSTRAINT schedule_occurrences_claim_shape_v2
    CHECK (
      status <> 'claimed'
      OR
      (claim_owner IS NOT NULL AND claim_command_id IS NOT NULL
       AND claim_expires_at IS NOT NULL AND claim_generation > 0)
    )
    """)

    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
    ADD CONSTRAINT backfill_windows_claim_shape_v2
    CHECK (
      status NOT IN ('claimed', 'running')
      OR
      (claim_owner IS NOT NULL AND claim_command_id IS NOT NULL
       AND claim_expires_at IS NOT NULL AND fencing_token > 0)
    )
    """)
  end

  def down do
    execute("""
    ALTER TABLE #{@prefix}.backfill_windows
    DROP CONSTRAINT IF EXISTS backfill_windows_claim_shape_v2
    """)

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
    DROP CONSTRAINT IF EXISTS schedule_occurrences_claim_shape_v2
    """)

    execute("""
    ALTER TABLE #{@prefix}.schedule_cursors
    DROP CONSTRAINT IF EXISTS schedule_cursors_claim_shape_v2
    """)

    execute("""
    ALTER TABLE #{@prefix}.materializations
    DROP CONSTRAINT IF EXISTS materializations_run_target_fk
    """)

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    DROP CONSTRAINT IF EXISTS materialization_claims_run_target_fk
    """)

    drop_if_exists(
      index(:run_targets, [], prefix: @prefix, name: :run_targets_claim_lineage_uidx)
    )
  end
end
