defmodule FavnStoragePostgres.Migrations.CreateStorageV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    execute("CREATE SCHEMA IF NOT EXISTS #{@prefix}")

    create table(:workspaces, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, primary_key: true)
      add(:slug, :text, null: false)
      add(:display_name, :text, null: false)
      add(:status, :text, null: false, default: "active")
      add(:version, :bigint, null: false, default: 1)
      timestamps(type: :timestamptz)
    end

    create(unique_index(:workspaces, [:slug], prefix: @prefix))

    create(
      constraint(:workspaces, :workspaces_id_valid,
        prefix: @prefix,
        check: "octet_length(workspace_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:workspaces, :workspaces_slug_valid,
        prefix: @prefix,
        check: "slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'"
      )
    )

    create(
      constraint(:workspaces, :workspaces_display_name_valid,
        prefix: @prefix,
        check: "octet_length(display_name) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:workspaces, :workspaces_status_valid,
        prefix: @prefix,
        check: "status IN ('active', 'suspended', 'retired')"
      )
    )

    create table(:manifest_versions, prefix: @prefix, primary_key: false) do
      add(:manifest_version_id, :text, primary_key: true)
      add(:content_hash, :binary, null: false)
      add(:schema_version, :integer, null: false)
      add(:runner_contract_version, :integer, null: false)
      add(:payload_version, :smallint, null: false)
      add(:manifest, :map, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(unique_index(:manifest_versions, [:content_hash], prefix: @prefix))

    create(
      index(:manifest_versions, [{:desc, :inserted_at}, {:desc, :manifest_version_id}],
        prefix: @prefix,
        name: :manifest_versions_history_idx
      )
    )

    create(
      constraint(:manifest_versions, :manifest_versions_id_valid,
        prefix: @prefix,
        check: "octet_length(manifest_version_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:manifest_versions, :manifest_versions_versions_valid,
        prefix: @prefix,
        check: "schema_version > 0 AND runner_contract_version > 0 AND payload_version > 0"
      )
    )

    create table(:execution_packages, prefix: @prefix, primary_key: false) do
      add(:content_hash, :binary, primary_key: true)
      add(:asset_module, :text, null: false)
      add(:asset_name, :text, null: false)
      add(:payload, :map, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      constraint(:execution_packages, :execution_packages_hash_valid,
        prefix: @prefix,
        check: "octet_length(content_hash) = 32"
      )
    )

    create(
      constraint(:execution_packages, :execution_packages_asset_ref_valid,
        prefix: @prefix,
        check:
          "octet_length(asset_module) BETWEEN 1 AND 255 AND octet_length(asset_name) BETWEEN 1 AND 255"
      )
    )

    create table(:manifest_execution_packages, prefix: @prefix, primary_key: false) do
      add(
        :manifest_version_id,
        references(:manifest_versions,
          prefix: @prefix,
          column: :manifest_version_id,
          type: :text,
          on_delete: :delete_all
        ),
        null: false,
        primary_key: true
      )

      add(
        :package_hash,
        references(:execution_packages,
          prefix: @prefix,
          column: :content_hash,
          type: :binary,
          on_delete: :restrict
        ),
        null: false,
        primary_key: true
      )

      add(:asset_module, :text, null: false)
      add(:asset_name, :text, null: false)
    end

    create(index(:manifest_execution_packages, [:package_hash], prefix: @prefix))

    create(
      unique_index(
        :manifest_execution_packages,
        [:manifest_version_id, :asset_module, :asset_name],
        prefix: @prefix,
        name: :manifest_execution_packages_asset_uidx
      )
    )

    create table(:workspace_deployments, prefix: @prefix, primary_key: false) do
      add(
        :workspace_id,
        references(:workspaces,
          prefix: @prefix,
          column: :workspace_id,
          type: :text,
          on_delete: :restrict
        ),
        null: false,
        primary_key: true
      )

      add(:deployment_id, :text, null: false, primary_key: true)

      add(
        :manifest_version_id,
        references(:manifest_versions,
          prefix: @prefix,
          column: :manifest_version_id,
          type: :text,
          on_delete: :restrict
        ),
        null: false
      )

      add(:configuration, :map, null: false)
      add(:configuration_fingerprint, :binary, null: false)
      add(:target_catalog_fingerprint, :binary, null: false)
      add(:configuration_version, :integer, null: false)
      add(:deployed_by_actor_id, :text)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(
        :workspace_deployments,
        [
          :workspace_id,
          :manifest_version_id,
          :configuration_fingerprint,
          :target_catalog_fingerprint
        ],
        prefix: @prefix,
        name: :workspace_deployments_content_uidx
      )
    )

    create(
      unique_index(
        :workspace_deployments,
        [:workspace_id, :deployment_id, :manifest_version_id],
        prefix: @prefix,
        name: :workspace_deployments_manifest_uidx
      )
    )

    create(
      constraint(:workspace_deployments, :workspace_deployments_values_valid,
        prefix: @prefix,
        check: "octet_length(deployment_id) BETWEEN 1 AND 255 AND configuration_version > 0"
      )
    )

    create table(:workspace_deployment_targets, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false, primary_key: true)
      add(:target_kind, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:selection_source, :text, null: false)
      add(:customer_visible, :boolean, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.workspace_deployment_targets
    ADD CONSTRAINT workspace_deployment_targets_deployment_fk
    FOREIGN KEY (workspace_id, deployment_id)
    REFERENCES #{@prefix}.workspace_deployments(workspace_id, deployment_id)
    ON DELETE RESTRICT
    """)

    create(
      index(
        :workspace_deployment_targets,
        [:workspace_id, :deployment_id, :target_kind, :target_id],
        prefix: @prefix,
        name: :workspace_deployment_targets_customer_idx,
        where: "customer_visible"
      )
    )

    create(
      constraint(:workspace_deployment_targets, :workspace_deployment_targets_kind_valid,
        prefix: @prefix,
        check: "target_kind IN ('asset', 'pipeline')"
      )
    )

    create(
      constraint(:workspace_deployment_targets, :workspace_deployment_targets_source_valid,
        prefix: @prefix,
        check: "selection_source IN ('common', 'explicit', 'dependency')"
      )
    )

    create table(:workspace_runtime_state, prefix: @prefix, primary_key: false) do
      add(
        :workspace_id,
        references(:workspaces,
          prefix: @prefix,
          column: :workspace_id,
          type: :text,
          on_delete: :restrict
        ),
        primary_key: true
      )

      add(:active_deployment_id, :text)
      add(:revision, :bigint, null: false, default: 0)
      add(:activated_by_actor_id, :text)
      add(:activated_at, :timestamptz)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.workspace_runtime_state
    ADD CONSTRAINT workspace_runtime_state_deployment_fk
    FOREIGN KEY (workspace_id, active_deployment_id)
    REFERENCES #{@prefix}.workspace_deployments(workspace_id, deployment_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      constraint(:workspace_runtime_state, :workspace_runtime_state_revision_valid,
        prefix: @prefix,
        check: "revision >= 0"
      )
    )

    create table(:outbox_events, prefix: @prefix, primary_key: false) do
      add(:outbox_event_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")

      add(
        :workspace_id,
        references(:workspaces,
          prefix: @prefix,
          column: :workspace_id,
          type: :text,
          on_delete: :restrict
        ),
        null: false
      )

      add(:command_id, :text, null: false)
      add(:event_kind, :text, null: false)
      add(:aggregate_kind, :text, null: false)
      add(:aggregate_id, :text, null: false)
      add(:aggregate_version, :bigint, null: false)
      add(:payload_version, :smallint, null: false)
      add(:payload, :map, null: false)
      add(:payload_hash, :binary, null: false)
      add(:occurred_at, :timestamptz, null: false)
      add(:publication_id, :bigint)
      add(:published_at, :timestamptz)
      add(:inserted_at, :timestamptz, null: false, default: fragment("clock_timestamp()"))
    end

    create(
      unique_index(:outbox_events, [:workspace_id, :outbox_event_id],
        prefix: @prefix,
        name: :outbox_events_workspace_uidx
      )
    )

    create(
      unique_index(:outbox_events, [:workspace_id, :command_id],
        prefix: @prefix,
        name: :outbox_events_command_uidx
      )
    )

    create(
      unique_index(:outbox_events, [:publication_id],
        prefix: @prefix,
        name: :outbox_events_publication_uidx,
        where: "publication_id IS NOT NULL"
      )
    )

    create(
      index(:outbox_events, [:outbox_event_id],
        prefix: @prefix,
        name: :outbox_events_unsequenced_idx,
        where: "publication_id IS NULL"
      )
    )

    create(
      index(:outbox_events, [:workspace_id, :publication_id],
        prefix: @prefix,
        name: :outbox_events_workspace_publication_idx,
        where: "publication_id IS NOT NULL"
      )
    )

    create(
      constraint(:outbox_events, :outbox_events_versions_valid,
        prefix: @prefix,
        check: "aggregate_version > 0 AND payload_version > 0"
      )
    )

    create table(:outbox_publication_state, prefix: @prefix, primary_key: false) do
      add(:singleton_id, :smallint, primary_key: true)
      add(:last_publication_id, :bigint, null: false, default: 0)
      add(:lease_owner, :text)
      add(:lease_generation, :bigint, null: false, default: 0)
      add(:lease_expires_at, :timestamptz)
      add(:updated_at, :timestamptz, null: false)
    end

    create(
      constraint(:outbox_publication_state, :outbox_publication_state_singleton,
        prefix: @prefix,
        check: "singleton_id = 1 AND last_publication_id >= 0 AND lease_generation >= 0"
      )
    )

    execute("""
    INSERT INTO #{@prefix}.outbox_publication_state
      (singleton_id, last_publication_id, lease_generation, updated_at)
    VALUES (1, 0, 0, clock_timestamp())
    """)

    create table(:runs, prefix: @prefix, primary_key: false) do
      add(
        :workspace_id,
        references(:workspaces,
          prefix: @prefix,
          column: :workspace_id,
          type: :text,
          on_delete: :restrict
        ),
        primary_key: true
      )

      add(:run_id, :text, primary_key: true)
      add(:deployment_id, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:root_execution_group_id, :text, null: false)
      add(:parent_run_id, :text)
      add(:rerun_of_run_id, :text)
      add(:submit_kind, :text, null: false)
      add(:trigger_type, :text, null: false)
      add(:status, :text, null: false)
      add(:event_sequence, :integer, null: false)
      add(:submitted_event_id, :bigint, null: false)
      add(:latest_event_id, :bigint, null: false)
      add(:snapshot_version, :smallint, null: false)
      add(:creation_hash, :binary, null: false)
      add(:snapshot_hash, :binary, null: false)
      add(:snapshot, :map, null: false)
      add(:inserted_at, :timestamptz, null: false)
      add(:updated_at, :timestamptz, null: false)
      add(:terminal_at, :timestamptz)
    end

    create(
      unique_index(:runs, [:workspace_id, :run_id, :deployment_id, :manifest_version_id],
        prefix: @prefix,
        name: :runs_deployment_manifest_uidx
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_deployment_manifest_fk
    FOREIGN KEY (workspace_id, deployment_id, manifest_version_id)
    REFERENCES #{@prefix}.workspace_deployments(workspace_id, deployment_id, manifest_version_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      index(:runs, [:workspace_id, {:desc, :latest_event_id}, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_recent_idx
      )
    )

    create(
      index(:runs, [{:desc, :latest_event_id}, :workspace_id, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_platform_recent_idx
      )
    )

    create(
      index(
        :runs,
        [:workspace_id, :manifest_version_id, {:desc, :latest_event_id}, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_manifest_recent_idx
      )
    )

    create(
      index(
        :runs,
        [:workspace_id, :root_execution_group_id, {:desc, :submitted_event_id}, {:desc, :run_id}],
        prefix: @prefix,
        name: :runs_group_children_idx
      )
    )

    create(
      index(:runs, [:workspace_id, :status, :run_id],
        prefix: @prefix,
        name: :runs_active_idx,
        where: "status IN ('pending', 'running')"
      )
    )

    create(
      index(:runs, [:workspace_id, :parent_run_id],
        prefix: @prefix,
        name: :runs_parent_idx,
        where: "parent_run_id IS NOT NULL"
      )
    )

    create(
      constraint(:runs, :runs_status_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'running', 'ok', 'partial', 'error', 'cancelled', 'timed_out')"
      )
    )

    create(
      constraint(:runs, :runs_values_valid,
        prefix: @prefix,
        check: "event_sequence > 0 AND snapshot_version > 0"
      )
    )

    create table(:run_events, prefix: @prefix, primary_key: false) do
      add(:event_id, :bigint, primary_key: true, generated: "BY DEFAULT AS IDENTITY")
      add(:workspace_id, :text, null: false)
      add(:run_id, :text, null: false)
      add(:sequence, :integer, null: false)
      add(:event_type, :text, null: false)
      add(:entity_type, :text, null: false)
      add(:asset_step_id, :text)
      add(:status, :text)
      add(:stage, :integer)
      add(:occurred_at, :timestamptz, null: false)
      add(:payload_version, :smallint, null: false)
      add(:event, :map, null: false)
      add(:event_hash, :binary, null: false)
      add(:outbox_event_id, :bigint, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    create(
      unique_index(:run_events, [:workspace_id, :event_id],
        prefix: @prefix,
        name: :run_events_workspace_event_uidx
      )
    )

    create(
      unique_index(:run_events, [:workspace_id, :run_id, :sequence],
        prefix: @prefix,
        name: :run_events_run_sequence_uidx
      )
    )

    create(unique_index(:run_events, [:outbox_event_id], prefix: @prefix))

    create(
      index(:run_events, [:workspace_id, :run_id, :event_type, :sequence],
        prefix: @prefix,
        name: :run_events_type_cursor_idx
      )
    )

    create(
      index(:run_events, [:workspace_id, :run_id, :asset_step_id, :sequence],
        prefix: @prefix,
        name: :run_events_step_cursor_idx,
        where: "asset_step_id IS NOT NULL"
      )
    )

    create(
      constraint(:run_events, :run_events_values_valid,
        prefix: @prefix,
        check: "sequence > 0 AND payload_version > 0 AND entity_type IN ('run', 'step')"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.run_events
    ADD CONSTRAINT run_events_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.run_events
    ADD CONSTRAINT run_events_outbox_fk
    FOREIGN KEY (workspace_id, outbox_event_id)
    REFERENCES #{@prefix}.outbox_events(workspace_id, outbox_event_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_root_fk
    FOREIGN KEY (workspace_id, root_execution_group_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_parent_fk
    FOREIGN KEY (workspace_id, parent_run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_rerun_fk
    FOREIGN KEY (workspace_id, rerun_of_run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_submitted_event_fk
    FOREIGN KEY (workspace_id, submitted_event_id)
    REFERENCES #{@prefix}.run_events(workspace_id, event_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    execute("""
    ALTER TABLE #{@prefix}.runs
    ADD CONSTRAINT runs_latest_event_fk
    FOREIGN KEY (workspace_id, latest_event_id)
    REFERENCES #{@prefix}.run_events(workspace_id, event_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create table(:run_targets, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false, primary_key: true)
      add(:deployment_id, :text, null: false)
      add(:manifest_version_id, :text, null: false)
      add(:target_kind, :text, null: false, primary_key: true)
      add(:target_id, :text, null: false, primary_key: true)
      add(:target_module, :text, null: false)
      add(:target_name, :text)
      add(:is_primary, :boolean, null: false, default: false)
      add(:submitted_event_id, :bigint, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.run_targets
    ADD CONSTRAINT run_targets_run_fk
    FOREIGN KEY (workspace_id, run_id, deployment_id, manifest_version_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id, deployment_id, manifest_version_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.run_targets
    ADD CONSTRAINT run_targets_deployment_target_fk
    FOREIGN KEY (workspace_id, deployment_id, target_kind, target_id)
    REFERENCES #{@prefix}.workspace_deployment_targets(workspace_id, deployment_id, target_kind, target_id)
    ON DELETE RESTRICT
    """)

    execute("""
    ALTER TABLE #{@prefix}.run_targets
    ADD CONSTRAINT run_targets_submitted_event_fk
    FOREIGN KEY (workspace_id, submitted_event_id)
    REFERENCES #{@prefix}.run_events(workspace_id, event_id)
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED
    """)

    create(
      index(
        :run_targets,
        [
          :workspace_id,
          :deployment_id,
          :target_kind,
          :target_id,
          {:desc, :submitted_event_id},
          {:desc, :run_id}
        ],
        prefix: @prefix,
        name: :run_targets_history_idx,
        include: [:is_primary]
      )
    )

    create(
      constraint(:run_targets, :run_targets_kind_valid,
        prefix: @prefix,
        check: "target_kind IN ('asset', 'pipeline')"
      )
    )

    create table(:runtime_input_pins, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false, primary_key: true)
      add(:node_key_hash, :binary, null: false, primary_key: true)
      add(:payload_fingerprint, :binary, null: false)

      add(
        :execution_package_hash,
        references(:execution_packages,
          prefix: @prefix,
          column: :content_hash,
          type: :binary,
          on_delete: :restrict
        ),
        null: false
      )

      add(:resolver_module, :text, null: false)
      add(:encryption_key_version, :integer, null: false)
      add(:payload, :binary, null: false)
      add(:inserted_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.runtime_input_pins
    ADD CONSTRAINT runtime_input_pins_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    create(
      constraint(:runtime_input_pins, :runtime_input_pins_key_version_valid,
        prefix: @prefix,
        check: "encryption_key_version > 0"
      )
    )

    create(index(:runtime_input_pins, [:execution_package_hash], prefix: @prefix))

    create table(:run_ownerships, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false, primary_key: true)
      add(:owner_id, :text)
      add(:fencing_token, :bigint, null: false, default: 0)
      add(:claim_command_id, :text)
      add(:last_renewal_id, :text)
      add(:expires_at, :timestamptz)
      add(:released_at, :timestamptz)
      add(:updated_at, :timestamptz, null: false)
    end

    execute("""
    ALTER TABLE #{@prefix}.run_ownerships
    ADD CONSTRAINT run_ownerships_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:run_ownerships, [:expires_at, :workspace_id, :run_id],
        prefix: @prefix,
        name: :run_ownerships_recovery_idx,
        where: "released_at IS NULL"
      )
    )

    create(
      constraint(:run_ownerships, :run_ownerships_fence_valid,
        prefix: @prefix,
        check: "fencing_token >= 0"
      )
    )

    create table(:runner_executions, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:runner_execution_id, :text, null: false, primary_key: true)
      add(:run_id, :text, null: false)
      add(:dispatch_id, :text, null: false)
      add(:last_command_id, :text, null: false)
      add(:owner_id, :text, null: false)
      add(:run_fencing_token, :bigint, null: false)
      add(:status, :text, null: false)
      add(:version, :bigint, null: false, default: 1)
      add(:dispatch_payload, :map, null: false)
      add(:result, :map)
      add(:error, :map)
      add(:dispatched_at, :timestamptz)
      add(:terminal_at, :timestamptz)
      timestamps(type: :timestamptz)
    end

    create(unique_index(:runner_executions, [:workspace_id, :dispatch_id], prefix: @prefix))

    execute("""
    ALTER TABLE #{@prefix}.runner_executions
    ADD CONSTRAINT runner_executions_run_fk
    FOREIGN KEY (workspace_id, run_id)
    REFERENCES #{@prefix}.runs(workspace_id, run_id)
    ON DELETE RESTRICT
    """)

    create(
      index(:runner_executions, [:workspace_id, :run_id, {:desc, :inserted_at}],
        prefix: @prefix,
        name: :runner_executions_run_idx
      )
    )

    create(
      index(:runner_executions, [:owner_id, :status, :workspace_id, :runner_execution_id],
        prefix: @prefix,
        name: :runner_executions_owner_active_idx,
        where: "terminal_at IS NULL"
      )
    )

    create(
      constraint(:runner_executions, :runner_executions_values_valid,
        prefix: @prefix,
        check:
          "run_fencing_token > 0 AND version > 0 AND status IN ('dispatching', 'running', 'cancelling', 'ok', 'error', 'cancelled', 'timed_out')"
      )
    )
  end

  def down do
    execute("DROP SCHEMA IF EXISTS #{@prefix} CASCADE")
  end
end
