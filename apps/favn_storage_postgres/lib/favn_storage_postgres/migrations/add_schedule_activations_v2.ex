defmodule FavnStoragePostgres.Migrations.AddScheduleActivationsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def up do
    alter table(:schedule_cursors, prefix: @prefix) do
      modify(:next_due_at, :timestamptz, null: true, from: {:timestamptz, null: false})
    end

    create table(:schedule_activations, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:pipeline_target_id, :text, null: false, primary_key: true)
      add(:schedule_id, :text, null: false, primary_key: true)
      add(:enabled, :boolean, null: false, default: false)
      add(:approved_schedule_fingerprint, :text)
      add(:version, :bigint, null: false, default: 1)
      add(:actor_id, :text, null: false)
      add(:reason, :text, null: false)
      add(:last_command_id, :text, null: false)
      add(:request_hash, :binary, null: false)
      add(:decided_at, :timestamptz, null: false)
      timestamps(type: :timestamptz)
    end

    create(
      unique_index(:schedule_activations, [:workspace_id, :last_command_id],
        prefix: @prefix,
        name: :schedule_activations_command_uidx
      )
    )

    create(
      constraint(:schedule_activations, :schedule_activations_values_valid,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(pipeline_target_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(schedule_id) BETWEEN 1 AND 255 AND " <>
            "version > 0 AND octet_length(actor_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(reason) BETWEEN 1 AND 2048 AND " <>
            "octet_length(last_command_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(request_hash) = 32 AND " <>
            "(approved_schedule_fingerprint IS NULL OR " <>
            "octet_length(approved_schedule_fingerprint) BETWEEN 1 AND 255)"
      )
    )

    create(
      constraint(:schedule_activations, :schedule_activations_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(pipeline_target_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(schedule_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(actor_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(last_command_id) BETWEEN 1 AND 255"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.schedule_activations
    ADD CONSTRAINT schedule_activations_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    create table(:schedule_activation_commands, prefix: @prefix, primary_key: false) do
      add(:workspace_id, :text, null: false, primary_key: true)
      add(:command_id, :text, null: false, primary_key: true)
      add(:request_hash, :binary, null: false)
      add(:result, :map, null: false)
      timestamps(type: :timestamptz)
    end

    create(
      constraint(:schedule_activation_commands, :schedule_activation_commands_values_valid,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(command_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(request_hash) = 32 AND " <>
            "octet_length(result::text) BETWEEN 2 AND 65536"
      )
    )

    create(
      constraint(
        :schedule_activation_commands,
        :schedule_activation_commands_identifier_lengths_v2,
        prefix: @prefix,
        check:
          "octet_length(workspace_id) BETWEEN 1 AND 255 AND " <>
            "octet_length(command_id) BETWEEN 1 AND 255"
      )
    )

    create(
      constraint(:schedule_activation_commands, :schedule_activation_commands_payload_bounds_v2,
        prefix: @prefix,
        check: "octet_length(result::text) BETWEEN 2 AND 65536"
      )
    )

    execute("""
    ALTER TABLE #{@prefix}.schedule_activation_commands
    ADD CONSTRAINT schedule_activation_commands_workspace_fk
    FOREIGN KEY (workspace_id)
    REFERENCES #{@prefix}.workspaces(workspace_id)
    ON DELETE RESTRICT
    """)

    drop(constraint(:schedule_occurrences, :schedule_occurrences_values_valid, prefix: @prefix))

    create(
      constraint(:schedule_occurrences, :schedule_occurrences_values_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'claimed', 'dispatching', 'completed', 'failed', 'suppressed') " <>
            "AND claim_generation >= 0 AND attempt_count >= 0"
      )
    )

    drop_if_exists(
      index(:schedule_occurrences, [],
        prefix: @prefix,
        name: :schedule_occurrences_dispatch_idx
      )
    )

    drop_if_exists(
      index(:schedule_occurrences, [],
        prefix: @prefix,
        name: :schedule_occurrences_workspace_dispatch_idx
      )
    )

    create(
      index(:schedule_occurrences, [:status, :due_at, :workspace_id, :occurrence_id],
        prefix: @prefix,
        name: :schedule_occurrences_dispatch_idx,
        where: "status IN ('pending', 'claimed', 'dispatching')"
      )
    )

    create(
      index(
        :schedule_occurrences,
        [:workspace_id, :status, :due_at, :occurrence_id],
        prefix: @prefix,
        name: :schedule_occurrences_workspace_dispatch_idx,
        where: "status IN ('pending', 'claimed', 'dispatching')"
      )
    )
  end

  def down do
    execute("""
    UPDATE #{@prefix}.schedule_occurrences
    SET status = 'failed',
        last_error = '{"kind":"migration_rollback","message":"schedule dispatch state rolled back"}'::jsonb,
        claim_owner = NULL,
        claim_command_id = NULL,
        claim_expires_at = NULL,
        updated_at = clock_timestamp()
    WHERE status IN ('suppressed', 'dispatching')
    """)

    drop(constraint(:schedule_occurrences, :schedule_occurrences_values_valid, prefix: @prefix))

    create(
      constraint(:schedule_occurrences, :schedule_occurrences_values_valid,
        prefix: @prefix,
        check:
          "status IN ('pending', 'claimed', 'completed', 'failed') " <>
            "AND claim_generation >= 0 AND attempt_count >= 0"
      )
    )

    drop(
      index(:schedule_occurrences, [],
        prefix: @prefix,
        name: :schedule_occurrences_dispatch_idx
      )
    )

    drop(
      index(:schedule_occurrences, [],
        prefix: @prefix,
        name: :schedule_occurrences_workspace_dispatch_idx
      )
    )

    create(
      index(:schedule_occurrences, [:status, :due_at, :workspace_id, :occurrence_id],
        prefix: @prefix,
        name: :schedule_occurrences_dispatch_idx,
        where: "status IN ('pending', 'claimed')"
      )
    )

    create(
      index(
        :schedule_occurrences,
        [:workspace_id, :status, :due_at, :occurrence_id],
        prefix: @prefix,
        name: :schedule_occurrences_workspace_dispatch_idx,
        where: "status IN ('pending', 'claimed')"
      )
    )

    drop(table(:schedule_activation_commands, prefix: @prefix))
    drop(table(:schedule_activations, prefix: @prefix))

    execute("""
    UPDATE #{@prefix}.schedule_cursors
    SET next_due_at = updated_at
    WHERE next_due_at IS NULL
    """)

    alter table(:schedule_cursors, prefix: @prefix) do
      modify(:next_due_at, :timestamptz, null: false, from: {:timestamptz, null: true})
    end
  end
end
