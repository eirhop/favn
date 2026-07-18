defmodule FavnStoragePostgres.Migrations.OptimizeSchedulerClaimsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"

  def change do
    create(
      index(
        :schedule_cursors,
        [:workspace_id, :next_due_at, :pipeline_target_id, :schedule_id],
        prefix: @prefix,
        name: :schedule_cursors_workspace_due_idx
      )
    )

    create(
      index(:schedule_cursors, [:workspace_id, :claim_command_id],
        prefix: @prefix,
        name: :schedule_cursors_claim_command_idx,
        where: "claim_command_id IS NOT NULL"
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

    create(
      index(:schedule_occurrences, [:workspace_id, :claim_command_id],
        prefix: @prefix,
        name: :schedule_occurrences_claim_command_idx,
        where: "claim_command_id IS NOT NULL"
      )
    )

    create(
      index(:auth_sessions, [:updated_at, :session_id],
        prefix: @prefix,
        name: :auth_sessions_inactive_retention_idx,
        where: "status <> 'active'"
      )
    )

    create(
      index(:auth_sessions, [:expires_at, :updated_at, :session_id],
        prefix: @prefix,
        name: :auth_sessions_expiry_retention_idx,
        where: "status = 'active'"
      )
    )

    create(
      index(:materialization_claims, [:status, :updated_at, :workspace_id, :claim_key],
        prefix: @prefix,
        name: :materialization_claims_retention_idx,
        where: "status IN ('succeeded', 'failed', 'expired')"
      )
    )

    create(
      index(:projection_failures, [:updated_at, :failure_id],
        prefix: @prefix,
        name: :projection_failures_retention_idx
      )
    )

    create(
      index(:log_entries, [:occurred_at, :log_id],
        prefix: @prefix,
        name: :log_entries_retention_idx
      )
    )

    create(
      index(:log_batches, [:inserted_at, :workspace_id, :batch_id],
        prefix: @prefix,
        name: :log_batches_retention_idx
      )
    )
  end
end
