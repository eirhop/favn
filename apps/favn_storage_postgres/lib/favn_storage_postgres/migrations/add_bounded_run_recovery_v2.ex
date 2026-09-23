defmodule FavnStoragePostgres.Migrations.AddBoundedRunRecoveryV2 do
  use Ecto.Migration

  def up do
    execute("""
    ALTER TABLE favn_control.runner_tasks ADD COLUMN cleanup_fencing_token bigint,
      ADD CONSTRAINT runner_tasks_cleanup_authority_valid CHECK (
        cleanup_fencing_token IS NULL OR (cleanup_fencing_token > 0 AND run_id IS NOT NULL
        AND task_kind IN ('relation_inspection','generation_capabilities','generation_marker_read')))
    """)

    execute("ALTER TABLE favn_control.target_operation_locks ADD COLUMN last_renewal_id text")

    execute("""
    ALTER TABLE favn_control.run_ownerships
      ADD COLUMN recovery_disposition text NOT NULL DEFAULT 'automatic',
      ADD COLUMN recovery_attempts integer NOT NULL DEFAULT 0,
      ADD COLUMN claim_purpose text NOT NULL DEFAULT 'execution',
      ADD COLUMN next_recovery_at timestamptz,
      ADD COLUMN attention_revision bigint,
      ADD COLUMN last_renewed_at timestamptz,
      ADD COLUMN diagnosis_reason text,
      ADD CONSTRAINT run_ownerships_recovery_valid CHECK (
        recovery_disposition IN ('automatic', 'attention') AND
        recovery_attempts BETWEEN 0 AND 3 AND
        claim_purpose IN ('execution', 'diagnosis', 'cleanup') AND
        (recovery_disposition <> 'attention' OR attention_revision IS NOT NULL))
    """)

    execute("""
    CREATE INDEX run_ownerships_eligible_recovery_idx
    ON favn_control.run_ownerships(workspace_id, next_recovery_at, run_id)
    WHERE recovery_disposition = 'automatic'
    """)
  end

  def down do
    raise "Run recovery state cannot be discarded by binary rollback; use a reviewed forward repair"
  end
end
