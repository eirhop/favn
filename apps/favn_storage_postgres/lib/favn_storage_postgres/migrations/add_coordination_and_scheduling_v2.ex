defmodule FavnStoragePostgres.Migrations.AddCoordinationAndSchedulingV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    create table(:schedule_cursors, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false, primary_key: true)
      add(:target_kind, :text, null: false, default: "pipeline")
      add(:pipeline_target_id, :text, null: false, primary_key: true)
      add(:schedule_id, :text, null: false, primary_key: true)
      add(:next_due_at, :timestamptz, null: false)
      add(:cursor, :map, null: false)
      add(:version, :bigint, null: false, default: 1)
      add(:claim_owner, :text)
      add(:claim_generation, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:last_command_id, :text)
      add(:claim_expires_at, :timestamptz)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.schedule_cursors
    ADD CONSTRAINT schedule_cursors_target_fk
    FOREIGN KEY (workspace_id, deployment_id, target_kind, pipeline_target_id)
    REFERENCES #{@prefix}.workspace_deployment_targets(workspace_id, deployment_id, target_kind, target_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:schedule_cursors, [:next_due_at, :workspace_id, :pipeline_target_id, :schedule_id],
        prefix: @prefix,
        name: :schedule_cursors_due_idx
      )
    )

    create(
      constraint(:schedule_cursors, :schedule_cursors_values_valid,
        prefix: @prefix,
        check: "target_kind = 'pipeline' AND version > 0 AND claim_generation >= 0"
      )
    )

    create table(:schedule_occurrences, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:occurrence_id, :text, null: false, primary_key: true)
      add(:occurrence_key, :binary, null: false)
      add(:evaluation_command_id, :text, null: false)
      add(:deployment_id, :text, null: false)
      add(:pipeline_target_id, :text, null: false)
      add(:schedule_id, :text, null: false)
      add(:due_at, :timestamptz, null: false)
      add(:payload, :map, null: false)
      add(:status, :text, null: false, default: "pending")
      add(:claim_owner, :text)
      add(:claim_generation, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:last_command_id, :text)
      add(:claim_expires_at, :timestamptz)
      add(:run_id, :text)
      add(:attempt_count, :integer, null: false, default: 0)
      add(:last_error, :map)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:schedule_occurrences, [:workspace_id, :occurrence_key],
        prefix: @prefix,
        name: :schedule_occurrences_key_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
    ADD CONSTRAINT schedule_occurrences_cursor_fk
    FOREIGN KEY (workspace_id, deployment_id, pipeline_target_id, schedule_id)
    REFERENCES #{@prefix}.schedule_cursors(workspace_id, deployment_id, pipeline_target_id, schedule_id)
    ON DELETE RESTRICT;
    """)

    execute("""
    ALTER TABLE #{@prefix}.schedule_occurrences
    ADD CONSTRAINT schedule_occurrences_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      index(:schedule_occurrences, [:status, :due_at, :workspace_id, :occurrence_id],
        prefix: @prefix,
        name: :schedule_occurrences_dispatch_idx,
        where: "status IN ('pending', 'claimed')"
      )
    )

    create(
      constraint(:schedule_occurrences, :schedule_occurrences_values_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'claimed', 'completed', 'failed') AND claim_generation >= 0 AND attempt_count >= 0"
      )
    )

    create table(:capacity_scopes, prefix: @prefix, primary_key: false) do
      add(:scope_id, :text, primary_key: true)
      add(:workspace_id, :text)
      add(:scope_kind, :text, null: false)
      add(:scope_key, :text, null: false)
      add(:capacity_limit, :integer, null: false)
      add(:active_count, :integer, null: false, default: 0)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.capacity_scopes
    ADD CONSTRAINT capacity_scopes_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    execute("""
    CREATE UNIQUE INDEX capacity_scopes_identity_uidx
    ON #{@prefix}.capacity_scopes (workspace_id, scope_kind, scope_key) NULLS NOT DISTINCT
    """)

    create(
      constraint(:capacity_scopes, :capacity_scopes_values_valid,
        prefix: @prefix,
        check:
          "capacity_limit > 0 AND active_count >= 0 AND active_count <= capacity_limit AND version > 0"
      )
    )

    create(
      constraint(:capacity_scopes, :capacity_scopes_scope_valid,
        prefix: @prefix,
        check:
          "(scope_kind = 'platform' AND workspace_id IS NULL) OR (scope_kind <> 'platform' AND workspace_id IS NOT NULL)"
      )
    )

    create table(:execution_leases, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:lease_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false)
      add(:step_id, :text, null: false)
      add(:command_id, :text, null: false)
      add(:request_hash, :binary, null: false)
      add(:owner_id, :text, null: false)
      add(:owner_generation, :bigint, null: false)
      add(:last_renewal_id, :text)
      add(:status, :text, null: false, default: "active")
      add(:expires_at, :timestamptz, null: false)
      add(:released_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.execution_leases
    ADD CONSTRAINT execution_leases_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    create(
      unique_index(:execution_leases, [:workspace_id, :command_id],
        prefix: @prefix,
        name: :execution_leases_command_uidx
      )
    )

    create(
      index(:execution_leases, [:expires_at, :workspace_id, :lease_id],
        prefix: @prefix,
        name: :execution_leases_expiry_idx,
        where: "status = 'active'"
      )
    )

    create(
      index(:execution_leases, [:workspace_id, :run_id, :status],
        prefix: @prefix,
        name: :execution_leases_run_idx
      )
    )

    create(
      constraint(:execution_leases, :execution_leases_values_valid,
        prefix: @prefix,
        check: "owner_generation > 0 AND status IN ('active', 'released', 'expired')"
      )
    )

    create table(:execution_lease_scopes, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:lease_id, :text, null: false, primary_key: true)
      add(:scope_id, :text, null: false, primary_key: true)
      add(:units, :integer, null: false, default: 1)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.execution_lease_scopes
    ADD CONSTRAINT execution_lease_scopes_lease_fk
    FOREIGN KEY (workspace_id, lease_id)
    REFERENCES #{@prefix}.execution_leases(workspace_id, lease_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.execution_lease_scopes
    ADD CONSTRAINT execution_lease_scopes_scope_fk
    FOREIGN KEY (scope_id)
    REFERENCES #{@prefix}.capacity_scopes(scope_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:execution_lease_scopes, [:scope_id, :workspace_id, :lease_id],
        prefix: @prefix,
        name: :execution_lease_scopes_scope_idx
      )
    )

    create(
      constraint(:execution_lease_scopes, :execution_lease_scopes_units_valid,
        prefix: @prefix,
        check: "units > 0"
      )
    )

    create table(:admission_waiters, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:waiter_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false)
      add(:step_id, :text, null: false)
      add(:command_id, :text, null: false)
      add(:request_hash, :binary, null: false)
      add(:requested_scopes, {:array, :map}, null: false)
      add(:blocking_scope_id, :text, null: false)
      add(:priority, :integer, null: false, default: 0)
      add(:status, :text, null: false, default: "waiting")
      add(:available_at, :timestamptz, null: false)
      add(:expires_at, :timestamptz, null: false)
      add(:claim_owner, :text)
      add(:claim_generation, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:claim_expires_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:admission_waiters, [:workspace_id, :run_id, :step_id],
        prefix: @prefix,
        name: :admission_waiters_step_uidx,
        where: "status IN ('waiting', 'claimed')"
      )
    )

    create(
      unique_index(:admission_waiters, [:workspace_id, :command_id],
        prefix: @prefix,
        name: :admission_waiters_command_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.admission_waiters
    ADD CONSTRAINT admission_waiters_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.admission_waiters
    ADD CONSTRAINT admission_waiters_scope_fk
    FOREIGN KEY (blocking_scope_id)
    REFERENCES #{@prefix}.capacity_scopes(scope_id)
    ON DELETE RESTRICT
    """)

    create(
      index(
        :admission_waiters,
        [:blocking_scope_id, {:desc, :priority}, :available_at, :waiter_id],
        prefix: @prefix,
        name: :admission_waiters_claim_idx,
        where: "status = 'waiting'"
      )
    )

    create(
      index(:admission_waiters, [:expires_at, :workspace_id, :waiter_id],
        prefix: @prefix,
        name: :admission_waiters_expiry_idx,
        where: "status IN ('waiting', 'claimed')"
      )
    )

    create(
      constraint(:admission_waiters, :admission_waiters_values_valid,
        prefix: @prefix,
        check:
          "status IN ('waiting', 'claimed', 'admitted', 'expired', 'cancelled') AND claim_generation >= 0"
      )
    )

    create table(:materialization_claims, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:claim_key, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false)
      add(:target_kind, :text, null: false)
      add(:target_id, :text, null: false)
      add(:partition_key, :text, null: false)
      add(:run_id, :text, null: false)
      add(:claim_command_id, :text, null: false)
      add(:claim_request_hash, :binary, null: false)
      add(:owner_id, :text, null: false)
      add(:fencing_token, :bigint, null: false)
      add(:last_renewal_id, :text)
      add(:last_finish_command_id, :text)
      add(:finish_hash, :binary)
      add(:status, :text, null: false, default: "claimed")
      add(:expires_at, :timestamptz, null: false)
      add(:completed_at, :timestamptz)
      add(:result, :map)
      add(:error, :map)
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    ADD CONSTRAINT materialization_claims_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.materialization_claims
    ADD CONSTRAINT materialization_claims_target_fk
    FOREIGN KEY (workspace_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.workspace_deployment_targets(workspace_id, deployment_id, target_kind, target_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:materialization_claims, [:expires_at, :workspace_id, :claim_key],
        prefix: @prefix,
        name: :materialization_claims_expiry_idx,
        where: "status = 'claimed'"
      )
    )

    create(
      unique_index(:materialization_claims, [:workspace_id, :claim_command_id],
        prefix: @prefix,
        name: :materialization_claims_command_uidx
      )
    )

    create(
      constraint(:materialization_claims, :materialization_claims_values_valid,
        prefix: @prefix,
        check:
          "fencing_token > 0 AND version > 0 AND status IN ('claimed', 'succeeded', 'failed', 'expired')"
      )
    )

    create table(:materializations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:materialization_id, :text, null: false, primary_key: true)
      add(:claim_key, :text, null: false)
      add(:deployment_id, :text, null: false)
      add(:target_kind, :text, null: false)
      add(:target_id, :text, null: false)
      add(:partition_key, :text, null: false)
      add(:run_id, :text, null: false)
      add(:payload, :map, null: false)
      add(:payload_hash, :binary, null: false)
      add(:outbox_event_id, :bigint, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(:materializations, [:workspace_id, :claim_key],
        prefix: @prefix,
        name: :materializations_claim_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.materializations
    ADD CONSTRAINT materializations_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.materializations
    ADD CONSTRAINT materializations_outbox_fk
    FOREIGN KEY (workspace_id, outbox_event_id)
    REFERENCES #{@prefix}.outbox_events(workspace_id, outbox_event_id)
    ON DELETE RESTRICT
    """)

    create(
      index(
        :materializations,
        [:workspace_id, :deployment_id, :target_id, :partition_key, {:desc, :inserted_at}],
        prefix: @prefix,
        name: :materializations_target_idx
      )
    )
  end

  def down do
    drop(table(:materializations, prefix: @prefix))
    drop(table(:materialization_claims, prefix: @prefix))
    drop(table(:admission_waiters, prefix: @prefix))
    drop(table(:execution_lease_scopes, prefix: @prefix))
    drop(table(:execution_leases, prefix: @prefix))
    drop(table(:capacity_scopes, prefix: @prefix))
    drop(table(:schedule_occurrences, prefix: @prefix))
    drop(table(:schedule_cursors, prefix: @prefix))
  end
end
