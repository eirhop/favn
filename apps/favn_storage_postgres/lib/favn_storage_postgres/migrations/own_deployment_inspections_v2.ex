defmodule FavnStoragePostgres.Migrations.OwnDeploymentInspectionsV2 do
  @moduledoc false
  use Ecto.Migration

  def up do
    execute("""
    CREATE TABLE favn_control.local_deployment_cancellations (
      workspace_id text NOT NULL REFERENCES favn_control.workspaces(workspace_id) ON DELETE RESTRICT,
      operation_id text NOT NULL,
      cancelled_at timestamptz NOT NULL,
      reason text NOT NULL,
      PRIMARY KEY (workspace_id, operation_id),
      CHECK (octet_length(operation_id) BETWEEN 1 AND 128 AND octet_length(reason) BETWEEN 1 AND 128)
    )
    """)

    execute("""
    ALTER TABLE favn_control.manifest_deployment_operations
      ALTER COLUMN archive_sha256 DROP NOT NULL,
      ADD COLUMN source text NOT NULL DEFAULT 'archive',
      ADD COLUMN local_session_id text,
      ADD COLUMN local_expires_at timestamptz,
      ADD COLUMN inspection_deadline_at timestamptz,
      ADD COLUMN cancellation_requested_at timestamptz,
      ADD COLUMN cleanup_state text NOT NULL DEFAULT 'pending',
      ADD COLUMN activation_receipt jsonb,
      ADD COLUMN expected_runtime_revision bigint,
      ADD COLUMN inspection_binding_hash bytea,
      ADD COLUMN cleanup_cursor text,
      ADD COLUMN request jsonb NOT NULL DEFAULT '{}'
    """)

    execute("""
    ALTER TABLE favn_control.runner_tasks ADD COLUMN deployment_operation_id text
    """)

    execute("""
    ALTER TABLE favn_control.runner_tasks ADD CONSTRAINT runner_tasks_deployment_owner_fk
      FOREIGN KEY (workspace_id, deployment_operation_id)
      REFERENCES favn_control.manifest_deployment_operations(workspace_id, operation_id)
      ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE favn_control.runner_tasks ADD CONSTRAINT runner_tasks_deployment_owner_kind
      CHECK (deployment_operation_id IS NULL OR
        (task_kind = 'relation_inspection' AND operation_id IS NULL AND run_id IS NULL))
    """)

    execute("""
    CREATE INDEX runner_tasks_deployment_owner_idx
      ON favn_control.runner_tasks(workspace_id, deployment_operation_id, task_id)
      INCLUDE (status) WHERE deployment_operation_id IS NOT NULL
    """)

    execute("""
    CREATE INDEX manifest_deployment_cleanup_idx
      ON favn_control.manifest_deployment_operations(cleanup_state, updated_at, workspace_id, operation_id)
    """)

    execute("""
    CREATE INDEX manifest_deployment_local_owner_idx
      ON favn_control.manifest_deployment_operations(workspace_id, source, accepted_at)
      WHERE source = 'local';
    """)

    execute(
      "ALTER TABLE favn_control.manifest_deployment_operations DROP CONSTRAINT manifest_deployment_operations_values_valid"
    )

    execute("""
    ALTER TABLE favn_control.manifest_deployment_operations
    ADD CONSTRAINT manifest_deployment_operations_values_valid CHECK (
        octet_length(workspace_id) BETWEEN 1 AND 255 AND
        octet_length(operation_id) BETWEEN 1 AND 128 AND
        operation_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$' AND
        octet_length(archive_sha256) = 32 AND
        octet_length(request_fingerprint) = 32 AND
        octet_length(service_identity) BETWEEN 1 AND 128 AND
        octet_length(manifest_version_id) BETWEEN 1 AND 255 AND
        octet_length(manifest_content_hash) = 32 AND
        jsonb_typeof(runner_releases) = 'object' AND
        state IN ('accepted', 'activating', 'succeeded', 'needs_attention', 'failed', 'unknown', 'cancelled', 'cancelling') AND
        (deployment_id IS NULL OR octet_length(deployment_id) BETWEEN 1 AND 255) AND
        (failure_class IS NULL OR octet_length(failure_class) BETWEEN 1 AND 255) AND
        (activation_diagnostics IS NULL OR octet_length(activation_diagnostics::text) <= 65536) AND
        claim_fence >= 0 AND
        inspection_total >= 0 AND
        inspection_completed BETWEEN 0 AND inspection_total AND
        ((claim_owner IS NULL AND claim_expires_at IS NULL) OR
         (octet_length(claim_owner) BETWEEN 1 AND 255 AND claim_expires_at IS NOT NULL)) AND
        ((state IN ('succeeded', 'needs_attention') AND deployment_id IS NOT NULL AND terminal_at IS NOT NULL) OR
         (state IN ('failed', 'unknown', 'cancelled') AND failure_class IS NOT NULL AND terminal_at IS NOT NULL) OR
         (state IN ('accepted', 'activating', 'cancelling') AND terminal_at IS NULL))
      )
    """)

    execute("""
    ALTER TABLE favn_control.manifest_deployment_operations ADD CONSTRAINT manifest_deployment_owner_valid CHECK (
      source IN ('archive', 'local') AND
      ((source = 'archive' AND archive_sha256 IS NOT NULL AND local_session_id IS NULL AND local_expires_at IS NULL)
       OR (source = 'local' AND archive_sha256 IS NULL AND local_session_id IS NOT NULL AND octet_length(local_session_id) BETWEEN 1 AND 128 AND local_expires_at IS NOT NULL)) AND
      cleanup_state IN ('pending', 'settling', 'settled', 'unknown') AND
      jsonb_typeof(request) = 'object' AND octet_length(request::text) <= 65536 AND
      (activation_receipt IS NULL OR (jsonb_typeof(activation_receipt) = 'object' AND octet_length(activation_receipt::text) <= 65536))
    );
    """)
  end

  def down do
    raise "deployment ownership requires a coordinated forward migration"
  end
end
