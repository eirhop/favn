defmodule FavnStoragePostgres.Migrations.HardenIdentifierBoundsV2 do
  @moduledoc false

  use Ecto.Migration

  @prefix "favn_control"
  @identifier_columns %{
    workspaces: ~w(workspace_id slug),
    manifest_versions: ~w(manifest_version_id),
    execution_packages: ~w(asset_module asset_name),
    manifest_execution_packages: ~w(manifest_version_id),
    workspace_deployments:
      ~w(workspace_id deployment_id manifest_version_id deployed_by_actor_id),
    workspace_deployment_targets: ~w(workspace_id deployment_id target_id),
    workspace_runtime_state: ~w(workspace_id active_deployment_id activated_by_actor_id),
    outbox_events: ~w(workspace_id command_id),
    runs:
      ~w(workspace_id run_id deployment_id manifest_version_id root_execution_group_id parent_run_id rerun_of_run_id),
    run_events: ~w(workspace_id run_id asset_step_id),
    run_targets:
      ~w(workspace_id run_id deployment_id manifest_version_id target_id target_module target_name),
    runtime_input_pins: ~w(workspace_id run_id),
    run_ownerships: ~w(workspace_id run_id owner_id claim_command_id last_renewal_id),
    runner_executions:
      ~w(workspace_id runner_execution_id run_id dispatch_id last_command_id owner_id),
    schedule_cursors:
      ~w(workspace_id deployment_id pipeline_target_id schedule_id claim_command_id last_command_id),
    schedule_occurrences:
      ~w(workspace_id occurrence_id evaluation_command_id deployment_id pipeline_target_id schedule_id claim_command_id last_command_id run_id),
    capacity_scopes: ~w(scope_id workspace_id scope_key),
    execution_leases:
      ~w(workspace_id lease_id run_id step_id command_id owner_id last_renewal_id),
    execution_lease_scopes: ~w(workspace_id lease_id scope_id),
    admission_waiters:
      ~w(workspace_id waiter_id run_id step_id command_id blocking_scope_id claim_command_id),
    materialization_claims:
      ~w(workspace_id claim_key deployment_id target_id partition_key run_id claim_command_id owner_id last_renewal_id last_finish_command_id),
    materializations:
      ~w(workspace_id materialization_id claim_key deployment_id target_id partition_key run_id),
    coverage_baselines: ~w(workspace_id baseline_id deployment_id manifest_version_id target_id),
    backfills:
      ~w(workspace_id backfill_id root_run_id start_command_id last_command_id deployment_id manifest_version_id target_id),
    backfill_plan_batches: ~w(workspace_id backfill_id command_id),
    backfill_windows:
      ~w(workspace_id backfill_id window_id window_key claim_command_id last_command_id run_id),
    projection_cursors: ~w(owner_id),
    projection_failures: ~w(workspace_id),
    execution_group_overviews: ~w(workspace_id root_run_id),
    backfill_overviews: ~w(workspace_id backfill_id),
    target_statuses: ~w(workspace_id deployment_id target_id run_id),
    asset_window_states:
      ~w(workspace_id manifest_version_id target_id window_key run_id materialization_id),
    asset_freshness_states:
      ~w(workspace_id deployment_id target_id freshness_key latest_attempt_materialization_id latest_success_materialization_id),
    log_batches: ~w(workspace_id batch_id command_id),
    log_entries: ~w(workspace_id batch_id run_id),
    auth_actors: ~w(actor_id normalized_username creation_command_id),
    auth_credentials: ~w(actor_id),
    auth_sessions: ~w(session_id actor_id creation_command_id),
    auth_workspace_memberships: ~w(workspace_id actor_id),
    auth_platform_grants: ~w(actor_id),
    auth_audit_entries: ~w(workspace_id command_id principal_id subject_id),
    auth_platform_audit_entries: ~w(command_id principal_id subject_id),
    idempotency_records: ~w(workspace_id principal_id),
    maintenance_jobs: ~w(job_id workspace_id owner_id)
  }

  def up do
    Enum.each(@identifier_columns, fn {table, columns} ->
      check =
        Enum.map_join(columns, " AND ", fn column ->
          "(#{column} IS NULL OR octet_length(#{column}) BETWEEN 1 AND 255)"
        end)

      create(
        constraint(table, constraint_name(table),
          prefix: @prefix,
          check: check
        )
      )
    end)

    create(
      constraint(:outbox_events, :outbox_events_aggregate_id_length_v2,
        prefix: @prefix,
        check: "octet_length(aggregate_id) BETWEEN 1 AND 512"
      )
    )
  end

  def down do
    drop(constraint(:outbox_events, :outbox_events_aggregate_id_length_v2, prefix: @prefix))

    @identifier_columns
    |> Map.keys()
    |> Enum.reverse()
    |> Enum.each(fn table ->
      drop(constraint(table, constraint_name(table), prefix: @prefix))
    end)
  end

  defp constraint_name(table), do: String.to_atom("#{table}_identifier_lengths_v2")
end
