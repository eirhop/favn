defmodule FavnStoragePostgres.StorageV2.Migrations do
  @moduledoc """
  Owns the reset-only Storage V2 migration baseline and exact schema diagnostics.

  Production runtime startup never calls `migrate!/1`. Deployments run it as a
  separate migrator role before starting application nodes.
  """

  alias Ecto.Adapters.SQL
  alias FavnStoragePostgres.Migrations.AddBackfillsAndProjectionsV2
  alias FavnStoragePostgres.Migrations.AddAssetAttemptOverviewsV2
  alias FavnStoragePostgres.Migrations.AddCommitSafeLogReplayV2
  alias FavnStoragePostgres.Migrations.AddDeploymentTargetDescriptorsV2
  alias FavnStoragePostgres.Migrations.AddExecutionPackageRuntimeInputResolverV2
  alias FavnStoragePostgres.Migrations.AddCoordinationAndSchedulingV2
  alias FavnStoragePostgres.Migrations.AddLogsIdentityAndOperationsV2
  alias FavnStoragePostgres.Migrations.AddRuntimeInputKeyInventoryV2
  alias FavnStoragePostgres.Migrations.AddRunnerReleaseIdentityV2
  alias FavnStoragePostgres.Migrations.AddResourceCircuitsV2
  alias FavnStoragePostgres.Migrations.AddScheduleOperatorReadsV2
  alias FavnStoragePostgres.Migrations.AddTargetGenerationFoundationV2
  alias FavnStoragePostgres.Migrations.CreateStorageV2
  alias FavnStoragePostgres.Migrations.EnforceRunPlanManifestIdentityV2
  alias FavnStoragePostgres.Migrations.HardenIdempotencyV2
  alias FavnStoragePostgres.Migrations.HardenIdentifierBoundsV2
  alias FavnStoragePostgres.Migrations.HardenClaimLineageV2
  alias FavnStoragePostgres.Migrations.HardenPayloadBoundsV2
  alias FavnStoragePostgres.Migrations.OptimizeSchedulerClaimsV2
  alias FavnStoragePostgres.Migrations.OptimizeManifestAndRunPlansV2
  alias FavnStoragePostgres.Migrations.OptimizeExecutionPackageRetentionV2
  alias FavnStoragePostgres.Migrations.OptimizeRunStatusPagingV2
  alias FavnStoragePostgres.Migrations.OptimizeRunnerExecutionPagingV2
  alias FavnStoragePostgres.Migrations.NormalizeResourceCircuitDefinitionsV2
  alias FavnStoragePostgres.Privileges

  @prefix "favn_control"
  @migrations [
    {20_260_717_000_000, CreateStorageV2},
    {20_260_717_010_000, AddCoordinationAndSchedulingV2},
    {20_260_717_020_000, AddBackfillsAndProjectionsV2},
    {20_260_717_030_000, AddLogsIdentityAndOperationsV2},
    {20_260_717_040_000, HardenIdentifierBoundsV2},
    {20_260_717_050_000, HardenIdempotencyV2},
    {20_260_717_060_000, HardenPayloadBoundsV2},
    {20_260_717_070_000, AddScheduleOperatorReadsV2},
    {20_260_717_080_000, HardenClaimLineageV2},
    {20_260_717_090_000, OptimizeSchedulerClaimsV2},
    {20_260_717_100_000, AddRuntimeInputKeyInventoryV2},
    {20_260_717_110_000, AddCommitSafeLogReplayV2},
    {20_260_717_120_000, AddDeploymentTargetDescriptorsV2},
    {20_260_717_130_000, OptimizeManifestAndRunPlansV2},
    {20_260_717_140_000, OptimizeRunnerExecutionPagingV2},
    {20_260_717_150_000, AddExecutionPackageRuntimeInputResolverV2},
    {20_260_717_160_000, OptimizeRunStatusPagingV2},
    {20_260_717_170_000, OptimizeExecutionPackageRetentionV2},
    {20_260_717_180_000, EnforceRunPlanManifestIdentityV2},
    {20_260_720_000_000, AddResourceCircuitsV2},
    {20_260_720_010_000, AddAssetAttemptOverviewsV2},
    {20_260_720_020_000, NormalizeResourceCircuitDefinitionsV2},
    {20_260_721_000_000, AddRunnerReleaseIdentityV2},
    {20_260_722_000_000, AddTargetGenerationFoundationV2}
  ]
  @required_tables ~w(
    schema_migrations
    workspaces
    manifest_versions
    execution_packages
    manifest_execution_packages
    workspace_deployments
    workspace_deployment_targets
    workspace_runtime_state
    outbox_events
    outbox_publication_state
    runs
    run_plans
    run_events
    run_targets
    runtime_input_pins
    runtime_input_key_versions
    run_ownerships
    runner_executions
    schedule_cursors
    schedule_occurrences
    capacity_scopes
    execution_leases
    execution_lease_scopes
    admission_waiters
    resource_circuits
    resource_circuit_outcomes
    resource_recovery_candidates
    materialization_claims
    materializations
    asset_target_generations
    asset_target_bindings
    rebuild_operations
    rebuild_plan_actions
    rebuild_windows
    target_operation_locks
    coverage_baselines
    backfills
    backfill_plan_batches
    backfill_windows
    projection_cursors
    projection_failures
    execution_group_overviews
    backfill_overviews
    target_statuses
    asset_window_states
    asset_freshness_states
    asset_attempt_overviews
    log_batches
    log_entries
    auth_actors
    auth_credentials
    auth_sessions
    auth_workspace_memberships
    auth_platform_grants
    auth_audit_entries
    auth_platform_audit_entries
    idempotency_records
    maintenance_jobs
  )
  @critical_indexes ~w(
    workspaces_slug_index
    manifest_versions_content_hash_index
    manifest_versions_history_idx
    execution_packages_unlinked_retention_idx
    manifest_execution_packages_package_hash_index
    manifest_execution_packages_asset_uidx
    workspace_deployments_content_uidx
    workspace_deployment_targets_customer_idx
    outbox_events_unsequenced_idx
    outbox_events_workspace_publication_idx
    outbox_events_command_uidx
    runs_recent_idx
    runs_platform_recent_idx
    runs_group_children_idx
    runs_plan_manifest_uidx
    runs_workspace_status_recent_idx
    runs_platform_status_recent_idx
    run_targets_history_idx
    run_targets_claim_lineage_uidx
    runtime_input_pins_execution_package_hash_index
    runtime_input_pins_key_version_idx
    run_ownerships_recovery_idx
    runner_executions_run_page_idx
    runner_executions_run_active_page_idx
    runner_executions_owner_active_idx
    runner_executions_workspace_active_idx
    schedule_cursors_due_idx
    schedule_cursors_workspace_due_idx
    schedule_cursors_claim_command_idx
    schedule_occurrences_dispatch_idx
    schedule_occurrences_workspace_dispatch_idx
    schedule_occurrences_claim_command_idx
    auth_sessions_inactive_retention_idx
    auth_sessions_expiry_retention_idx
    materialization_claims_retention_idx
    projection_failures_retention_idx
    log_entries_retention_idx
    log_batches_retention_idx
    log_batches_outbox_uidx
    log_entries_asset_idx
    log_entries_runner_idx
    log_entries_node_key_idx
    log_entries_asset_ref_idx
    schedule_occurrences_history_idx
    capacity_scopes_identity_uidx
    execution_leases_expiry_idx
    admission_waiters_claim_idx
    resource_circuits_probe_idx
    resource_circuit_outcomes_resource_idx
    resource_recovery_candidates_claim_idx
    materialization_claims_expiry_idx
    materializations_target_idx
    materializations_generation_idx
    asset_target_generations_command_uidx
    asset_target_generations_status_idx
    asset_target_bindings_status_idx
    rebuild_operations_idempotency_uidx
    rebuild_operations_recovery_idx
    rebuild_plan_actions_ordinal_uidx
    rebuild_windows_ordinal_uidx
    rebuild_windows_key_uidx
    rebuild_windows_claim_idx
    target_operation_locks_expiry_idx
    coverage_baselines_target_idx
    backfills_target_idx
    backfill_windows_page_idx
    backfill_windows_claim_idx
    backfill_windows_backfill_claim_idx
    projection_failures_event_uidx
    execution_group_overviews_recent_idx
    execution_group_overviews_platform_recent_idx
    backfill_overviews_status_idx
    target_statuses_status_idx
    asset_window_states_history_idx
    asset_freshness_states_node_idx
    asset_attempt_overviews_group_idx
    asset_attempt_overviews_run_idx
    log_entries_recent_idx
    log_entries_run_idx
    auth_actors_username_uidx
    auth_sessions_token_uidx
    auth_workspace_memberships_page_idx
    auth_audit_entries_page_idx
    auth_platform_audit_entries_page_idx
    idempotency_records_expiry_idx
    maintenance_jobs_queue_idx
  )
  @required_columns %{
    "schema_migrations" => ~w(version inserted_at),
    "admission_waiters" =>
      ~w(workspace_id waiter_id run_id step_id command_id request_hash requested_scopes blocking_scope_id priority status available_at expires_at claim_owner claim_generation claim_command_id claim_expires_at inserted_at updated_at),
    "asset_freshness_states" =>
      ~w(workspace_id evidence_generation_id deployment_id manifest_version_id target_id freshness_key latest_attempt_materialization_id latest_success_materialization_id latest_success_node_key_hash input_fingerprint status payload source_publication_id updated_at),
    "asset_attempt_overviews" =>
      ~w(workspace_id root_run_id run_id asset_step_id asset_ref window_identity window status stage attempt_number execution_pool queue_reason started_at finished_at duration_ms error output_metadata source_publication_id updated_at),
    "asset_window_states" =>
      ~w(workspace_id evidence_generation_id manifest_version_id target_id window_key window_start window_end status run_id materialization_id payload source_publication_id updated_at),
    "asset_target_generations" =>
      ~w(workspace_id target_id target_generation_id creating_manifest_id creation_command_id creating_descriptor_hash active_descriptor_hash logical_relation physical_relation physical_schema_fingerprint data_plane_marker activation_token status creating_rebuild_operation_id version created_at activated_at retired_at updated_at),
    "asset_target_bindings" =>
      ~w(workspace_id target_id active_generation_id desired_manifest_id desired_descriptor_hash compatibility_status reason_code compatibility_diff active_physical_fingerprint version updated_at),
    "auth_actors" =>
      ~w(actor_id username normalized_username display_name creation_command_id creation_hash status version inserted_at updated_at),
    "auth_audit_entries" =>
      ~w(audit_id workspace_id command_id principal_id action subject_kind subject_id detail occurred_at inserted_at),
    "auth_credentials" =>
      ~w(actor_id password_hash algorithm version changed_at inserted_at updated_at),
    "auth_platform_audit_entries" =>
      ~w(audit_id command_id principal_id action subject_kind subject_id detail occurred_at inserted_at),
    "auth_platform_grants" => ~w(actor_id roles status version inserted_at updated_at),
    "auth_sessions" =>
      ~w(session_id actor_id creation_command_id token_hash provider status expires_at revoked_at last_seen_at inserted_at updated_at),
    "auth_workspace_memberships" =>
      ~w(workspace_id actor_id roles status version inserted_at updated_at),
    "backfill_overviews" =>
      ~w(workspace_id backfill_id status total_count planned_count ready_count active_count succeeded_count failed_count cancelled_count source_publication_id updated_at),
    "backfill_plan_batches" =>
      ~w(workspace_id backfill_id batch_index command_id batch_hash window_count inserted_at),
    "backfill_windows" =>
      ~w(workspace_id backfill_id window_id batch_index window_key window_start window_end status claim_owner fencing_token claim_command_id last_command_id claim_expires_at run_id attempt_count last_error payload version inserted_at updated_at),
    "backfills" =>
      ~w(workspace_id backfill_id root_run_id start_command_id last_command_id request_hash deployment_id manifest_version_id target_kind target_id range_start range_end status expected_window_count expected_batch_count appended_window_count appended_batch_count plan_hash metadata version inserted_at updated_at),
    "capacity_scopes" =>
      ~w(scope_id workspace_id scope_kind scope_key capacity_limit active_count version inserted_at updated_at),
    "coverage_baselines" =>
      ~w(workspace_id baseline_id deployment_id manifest_version_id target_kind target_id coverage_start coverage_end evidence evidence_hash version inserted_at updated_at),
    "execution_group_overviews" =>
      ~w(workspace_id root_run_id status run_count pending_count running_count succeeded_count failed_count latest_event_id source_publication_id inserted_at updated_at),
    "execution_lease_scopes" => ~w(workspace_id lease_id scope_id units inserted_at),
    "execution_leases" =>
      ~w(workspace_id lease_id run_id step_id command_id request_hash owner_id owner_generation last_renewal_id status expires_at released_at inserted_at updated_at),
    "idempotency_records" =>
      ~w(workspace_id operation principal_kind principal_id key_hash request_fingerprint status response response_status expires_at inserted_at updated_at reservation_generation resource_kind resource_id),
    "log_batches" =>
      ~w(workspace_id batch_id command_id batch_hash entry_count outbox_event_id inserted_at),
    "log_entries" =>
      ~w(log_id workspace_id batch_id position run_id asset_step_id runner_execution_id node_key_hash asset_ref_hash stream source level message metadata occurred_at inserted_at),
    "maintenance_jobs" =>
      ~w(job_id job_kind scope_kind workspace_id status cursor configuration owner_id fencing_token claim_expires_at processed_count last_error version inserted_at updated_at),
    "manifest_versions" =>
      ~w(manifest_version_id content_hash schema_version runner_contract_version required_runner_release_id payload_version asset_count pipeline_count schedule_count atom_strings manifest inserted_at),
    "execution_packages" =>
      ~w(content_hash asset_module asset_name runtime_input_resolver payload first_linked_at inserted_at),
    "manifest_execution_packages" => ~w(manifest_version_id package_hash asset_module asset_name),
    "materialization_claims" =>
      ~w(workspace_id claim_key deployment_id target_kind target_id target_generation_id evidence_generation_id partition_key run_id claim_command_id claim_request_hash owner_id fencing_token last_renewal_id last_finish_command_id finish_hash status expires_at completed_at result error version inserted_at updated_at),
    "materializations" =>
      ~w(workspace_id materialization_id claim_key deployment_id target_kind target_id target_generation_id evidence_generation_id partition_key run_id payload payload_hash outbox_event_id inserted_at),
    "outbox_events" =>
      ~w(outbox_event_id workspace_id command_id event_kind aggregate_kind aggregate_id aggregate_version payload_version payload payload_hash occurred_at publication_id published_at inserted_at),
    "outbox_publication_state" =>
      ~w(singleton_id last_publication_id lease_owner lease_generation lease_expires_at updated_at),
    "projection_cursors" =>
      ~w(projector_name shard_id last_publication_id owner_id fencing_token claim_expires_at version updated_at),
    "projection_failures" =>
      ~w(failure_id projector_name shard_id publication_id workspace_id event_kind error_kind error_detail attempt_count inserted_at updated_at),
    "run_events" =>
      ~w(event_id workspace_id run_id sequence event_type entity_type asset_step_id status stage occurred_at payload_version event event_hash outbox_event_id inserted_at),
    "run_ownerships" =>
      ~w(workspace_id run_id owner_id fencing_token claim_command_id last_renewal_id expires_at released_at updated_at),
    "run_plans" =>
      ~w(workspace_id run_id manifest_version_id plan_version plan_hash plan inserted_at),
    "run_targets" =>
      ~w(workspace_id run_id deployment_id manifest_version_id target_kind target_id target_module target_name is_primary submitted_event_id inserted_at),
    "runner_executions" =>
      ~w(workspace_id runner_execution_id run_id dispatch_id last_command_id owner_id run_fencing_token status version dispatch_payload result error dispatched_at terminal_at inserted_at updated_at),
    "runs" =>
      ~w(workspace_id run_id deployment_id manifest_version_id root_execution_group_id parent_run_id rerun_of_run_id submit_kind trigger_type status event_sequence submitted_event_id latest_event_id snapshot_version creation_hash snapshot_hash snapshot inserted_at updated_at terminal_at),
    "runtime_input_pins" =>
      ~w(workspace_id run_id node_key_hash payload_fingerprint execution_package_hash resolver_module encryption_key_version payload inserted_at),
    "runtime_input_key_versions" => ~w(key_version first_used_at),
    "resource_circuits" =>
      ~w(workspace_id resource_kind resource_name state consecutive_failures failure_threshold probe_after_ms opened_at next_probe_at probe_owner_id probe_expires_at last_category last_outcome_at version inserted_at updated_at),
    "resource_circuit_outcomes" =>
      ~w(workspace_id outcome_id resource_kind resource_name run_id asset_step_id attempt status category occurred_at inserted_at),
    "resource_recovery_candidates" =>
      ~w(workspace_id candidate_id source_run_id node_key resource_kind resource_name reason status expires_at claim_owner claim_expires_at recovery_run_id inserted_at updated_at),
    "rebuild_operations" =>
      ~w(workspace_id operation_id root_target_id manifest_version_id active_generation_id candidate_generation_id plan_hash plan_version trigger actor_id session_id reason idempotency_key evaluated_at coverage_start coverage_end action_count window_count state phase activation_token dispatched_at result_marker unknown_outcome validation_result terminal_error cleanup_state version started_at completed_at cancelled_at inserted_at updated_at),
    "rebuild_plan_actions" =>
      ~w(workspace_id operation_id target_id ordinal action reason upstream_impact mapping_proof pinned_input_generation_ids candidate_generation_id status child_operation_id child_run_id version inserted_at updated_at),
    "rebuild_windows" =>
      ~w(workspace_id operation_id target_id item_id ordinal work_kind window_key window_start window_end status claim_owner fencing_token claim_command_id last_command_id claim_expires_at child_run_id materialization_id attempt_count row_count last_error candidate_generation_id version inserted_at updated_at),
    "schedule_cursors" =>
      ~w(workspace_id deployment_id target_kind pipeline_target_id schedule_id schedule_fingerprint definition next_due_at cursor version claim_owner claim_generation claim_command_id last_command_id claim_expires_at updated_at),
    "schedule_occurrences" =>
      ~w(workspace_id occurrence_id occurrence_key evaluation_command_id deployment_id pipeline_target_id schedule_id due_at payload status claim_owner claim_generation claim_command_id last_command_id claim_expires_at run_id attempt_count last_error inserted_at updated_at),
    "target_statuses" =>
      ~w(workspace_id deployment_id target_kind target_id status run_id event_id source_publication_id updated_at),
    "target_operation_locks" =>
      ~w(workspace_id target_id operation_id operation_type fencing_token lease_owner lease_expires_at version inserted_at updated_at),
    "workspace_deployment_targets" =>
      ~w(workspace_id deployment_id target_kind target_id selection_source customer_visible descriptor inserted_at),
    "workspace_deployments" =>
      ~w(workspace_id deployment_id manifest_version_id configuration configuration_fingerprint target_catalog_fingerprint configuration_version deployed_by_actor_id inserted_at),
    "workspace_runtime_state" =>
      ~w(workspace_id active_deployment_id revision activated_by_actor_id activated_at updated_at),
    "workspaces" => ~w(workspace_id slug display_name status version inserted_at updated_at)
  }
  @identifier_constraint_tables ~w(
    workspaces manifest_versions execution_packages manifest_execution_packages
    workspace_deployments workspace_deployment_targets
    workspace_runtime_state outbox_events runs run_plans run_events run_targets runtime_input_pins
    run_ownerships runner_executions schedule_cursors schedule_occurrences capacity_scopes
    execution_leases execution_lease_scopes admission_waiters materialization_claims
    materializations asset_target_generations asset_target_bindings rebuild_operations
    rebuild_plan_actions rebuild_windows target_operation_locks coverage_baselines backfills backfill_plan_batches backfill_windows
    projection_cursors projection_failures execution_group_overviews backfill_overviews
    target_statuses asset_window_states asset_freshness_states asset_attempt_overviews log_batches log_entries
    auth_actors auth_credentials auth_sessions auth_workspace_memberships auth_platform_grants
    auth_audit_entries auth_platform_audit_entries idempotency_records maintenance_jobs
  )
  @payload_constraint_tables ~w(
    manifest_versions execution_packages workspace_deployments outbox_events runs run_events runtime_input_pins
    runner_executions schedule_cursors schedule_occurrences admission_waiters
    materialization_claims materializations coverage_baselines backfills backfill_windows
    asset_target_generations asset_target_bindings rebuild_operations rebuild_plan_actions rebuild_windows
    projection_failures asset_window_states asset_freshness_states asset_attempt_overviews log_entries
    auth_audit_entries auth_platform_audit_entries maintenance_jobs
  )
  @critical_constraints ~w(
    workspaces_id_valid workspaces_slug_valid workspaces_display_name_valid workspaces_status_valid
    manifest_versions_id_valid manifest_versions_versions_valid
    manifest_versions_runner_release_valid execution_packages_hash_valid
    execution_packages_asset_ref_valid
    execution_packages_runtime_input_resolver_valid
    workspace_deployments_values_valid workspace_deployment_targets_kind_valid
    workspace_deployment_targets_source_valid workspace_runtime_state_revision_valid
    outbox_events_versions_valid outbox_events_aggregate_id_length_v2
    outbox_publication_state_singleton runs_status_valid runs_values_valid
    run_events_values_valid run_targets_kind_valid runtime_input_pins_key_version_valid
    runtime_input_key_versions_pkey runtime_input_key_versions_key_version_valid
    run_ownerships_fence_valid runner_executions_values_valid schedule_cursors_values_valid
    schedule_cursors_definition_bounded schedule_cursors_claim_shape_v2
    schedule_occurrences_claim_shape_v2
    schedule_occurrences_values_valid capacity_scopes_scope_valid capacity_scopes_values_valid
    execution_lease_scopes_units_valid execution_leases_values_valid
    admission_waiters_values_valid materialization_claims_values_valid
    materialization_claims_generation_valid materializations_generation_valid
    asset_target_generations_values_valid asset_target_bindings_values_valid
    rebuild_operations_values_valid rebuild_plan_actions_values_valid rebuild_windows_values_valid
    target_operation_locks_values_valid asset_window_states_evidence_generation_valid
    asset_freshness_states_evidence_generation_valid
    resource_circuits_values_valid resource_circuits_probe_shape_valid
    resource_circuit_outcomes_values_valid resource_recovery_candidates_values_valid
    coverage_baselines_values_valid backfills_values_valid backfill_plan_batches_values_valid
    asset_attempt_overviews_values_valid
    backfill_windows_values_valid backfill_windows_claim_shape_v2 projection_cursors_values_valid
    auth_actors_values_valid
    auth_credentials_values_valid auth_sessions_values_valid auth_workspace_memberships_values_valid
    auth_platform_grants_values_valid log_batches_count_valid log_entries_values_valid
    log_entries_filter_values_valid
    workspace_deployment_targets_descriptor_valid
    manifest_versions_summary_valid run_plans_values_valid
    idempotency_records_values_valid idempotency_records_payload_bounded maintenance_jobs_values_valid
    manifest_execution_packages_manifest_version_id_fkey
    manifest_execution_packages_package_hash_fkey workspace_runtime_state_deployment_fk
    runs_deployment_manifest_fk run_plans_run_manifest_fk run_plans_manifest_fk run_events_run_fk
    run_events_outbox_fk run_targets_run_fk run_targets_deployment_target_fk
    runtime_input_pins_run_fk runtime_input_pins_execution_package_hash_fkey
    run_ownerships_run_fk runner_executions_run_fk
    schedule_cursors_target_fk schedule_occurrences_cursor_fk execution_leases_run_fk
    execution_lease_scopes_lease_fk execution_lease_scopes_scope_fk admission_waiters_run_fk
    admission_waiters_scope_fk materialization_claims_run_fk materialization_claims_target_fk
    materialization_claims_run_target_fk materializations_run_fk materializations_outbox_fk
    materializations_run_target_fk materialization_claims_target_generation_fk
    materializations_target_generation_fk asset_target_generations_workspace_fk
    asset_target_generations_manifest_fk asset_target_generations_rebuild_operation_fk
    asset_target_bindings_workspace_fk asset_target_bindings_manifest_fk
    asset_target_bindings_active_generation_fk rebuild_operations_workspace_fk
    rebuild_operations_manifest_fk rebuild_operations_active_generation_fk
    rebuild_operations_candidate_generation_fk rebuild_plan_actions_operation_fk
    rebuild_plan_actions_candidate_generation_fk rebuild_plan_actions_child_run_fk
    rebuild_plan_actions_child_operation_fk
    rebuild_windows_action_fk rebuild_windows_candidate_generation_fk
    rebuild_windows_child_run_fk rebuild_windows_materialization_fk
    target_operation_locks_workspace_fk backfills_root_run_fk
    backfills_deployment_manifest_fk backfills_target_fk backfill_plan_batches_backfill_fk
    backfill_windows_batch_fk backfill_windows_run_fk auth_credentials_actor_fk
    auth_sessions_actor_fk auth_workspace_memberships_workspace_fk auth_workspace_memberships_actor_fk
  ) ++
                          Enum.map(@identifier_constraint_tables, &"#{&1}_identifier_lengths_v2") ++
                          Enum.map(@payload_constraint_tables, &"#{&1}_payload_bounds_v2")
  @expected_versions Enum.map(@migrations, fn {version, _module} -> version end)
  @expected_definition_fingerprint "282e61af233c25a872d7b7cb65f13410ba963001e8f8eed3904ccc4a8eff4e8d"

  @doc "Creates the V2 namespace and applies every known migration."
  @spec migrate!(module()) :: :ok
  def migrate!(repo) when is_atom(repo) do
    SQL.query!(repo, "CREATE SCHEMA IF NOT EXISTS #{@prefix}", [])

    {:ok, _migrated, _apps} =
      Ecto.Migrator.with_repo(repo, fn migrator_repo ->
        Ecto.Migrator.run(migrator_repo, @migrations, :up,
          all: true,
          prefix: @prefix
        )
      end)

    :ok
  end

  @doc "Returns the exact ordered Storage V2 migration versions required by this release."
  @spec expected_versions() :: [pos_integer()]
  def expected_versions, do: @expected_versions

  @doc "Returns exact, redacted compatibility evidence for runtime readiness."
  @spec diagnostics(module() | pid()) :: {:ok, map()} | {:error, term()}
  def diagnostics(repo) do
    with {:ok, server_version} <- server_version(repo),
         {:ok, tables} <- present_objects(repo, "r", @required_tables),
         {:ok, indexes} <- present_indexes(repo),
         {:ok, columns} <- present_columns(repo),
         {:ok, constraints} <- present_constraints(repo),
         {:ok, applied_versions} <- applied_versions(repo),
         {:ok, definition_fingerprint} <- definition_fingerprint(repo),
         {:ok, projection} <- projection_health(repo) do
      missing_tables = @required_tables -- tables
      missing_indexes = @critical_indexes -- indexes
      expected_columns = expected_columns()
      missing_columns = expected_columns -- columns
      unexpected_columns = columns -- expected_columns
      missing_constraints = @critical_constraints -- constraints
      missing_versions = @expected_versions -- applied_versions
      future_versions = applied_versions -- @expected_versions
      definition_matches? = definition_fingerprint == @expected_definition_fingerprint
      runtime_role = Privileges.current_role_diagnostics(repo)

      enforce_runtime_role? =
        Application.get_env(:favn_storage_postgres, :enforce_runtime_role, false)

      runtime_role_ready? = not enforce_runtime_role? or runtime_role.safe?

      status =
        status(
          server_version.major,
          missing_tables,
          missing_indexes,
          missing_columns,
          unexpected_columns,
          missing_constraints,
          missing_versions,
          future_versions,
          definition_matches?,
          projection.ready?,
          runtime_role_ready?
        )

      {:ok,
       %{
         status: status,
         ready?: status == :ready,
         engine: %{name: :postgresql, version: server_version},
         schema: @prefix,
         missing_tables: missing_tables,
         missing_critical_indexes: missing_indexes,
         missing_columns: missing_columns,
         unexpected_columns: unexpected_columns,
         missing_critical_constraints: missing_constraints,
         missing_migration_versions: missing_versions,
         future_migration_versions: future_versions,
         expected_migration_versions: @expected_versions,
         definition_fingerprint_matches?: definition_matches?,
         expected_definition_fingerprint: @expected_definition_fingerprint,
         actual_definition_fingerprint: definition_fingerprint,
         projection: projection,
         runtime_role: Map.put(runtime_role, :enforced?, enforce_runtime_role?)
       }}
    end
  end

  @doc "Returns true only for the exact supported schema and PostgreSQL major."
  @spec ready?(module() | pid()) :: boolean()
  def ready?(repo) do
    match?({:ok, %{ready?: true}}, diagnostics(repo))
  end

  defp server_version(repo) do
    case SQL.query(repo, "SHOW server_version_num", []) do
      {:ok, %{rows: [[value]]}} when is_binary(value) ->
        version = String.to_integer(value)

        {:ok,
         %{
           major: div(version, 10_000),
           minor: rem(version, 10_000),
           number: version
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp present_objects(repo, relkind, names) do
    case SQL.query(
           repo,
           """
           SELECT c.relname
           FROM pg_catalog.pg_class c
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND c.relkind = $2 AND c.relname = ANY($3::text[])
           ORDER BY c.relname
           """,
           [@prefix, relkind, names]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp present_indexes(repo) do
    case SQL.query(
           repo,
           """
           SELECT c.relname
           FROM pg_catalog.pg_class c
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
           WHERE n.nspname = $1 AND c.relkind = 'i'
             AND c.relname = ANY($2::text[])
             AND i.indisvalid AND i.indisready
           ORDER BY c.relname
           """,
           [@prefix, @critical_indexes]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [name] -> name end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp present_columns(repo) do
    case SQL.query(
           repo,
           """
           SELECT table_name || '.' || column_name
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = ANY($2::text[])
           ORDER BY table_name, ordinal_position
           """,
           [@prefix, @required_tables]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [column] -> column end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp present_constraints(repo) do
    case SQL.query(
           repo,
           """
           SELECT con.conname
           FROM pg_catalog.pg_constraint con
           JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND con.conname = ANY($2::text[])
             AND con.convalidated
           ORDER BY con.conname
           """,
           [@prefix, @critical_constraints]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [constraint] -> constraint end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expected_columns do
    @required_columns
    |> Enum.flat_map(fn {table, columns} -> Enum.map(columns, &"#{table}.#{&1}") end)
    |> Enum.sort()
  end

  defp applied_versions(repo) do
    case SQL.query(
           repo,
           "SELECT version FROM #{@prefix}.schema_migrations ORDER BY version",
           []
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [version] -> version end)}
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> {:ok, []}
      {:error, %Postgrex.Error{postgres: %{code: :invalid_schema_name}}} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp definition_fingerprint(repo) do
    with {:ok, columns} <-
           query_rows(
             repo,
             """
             SELECT c.relname, a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    a.attnotnull, a.attidentity, a.attgenerated,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid)
             FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_attrdef d
               ON d.adrelid = a.attrelid AND d.adnum = a.attnum
             WHERE n.nspname = $1 AND c.relname = ANY($2::text[])
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY c.relname, a.attname
             """,
             [@prefix, @required_tables]
           ),
         {:ok, constraints} <-
           query_rows(
             repo,
             """
             SELECT c.relname, con.conname, con.contype, con.convalidated,
                    con.condeferrable, con.condeferred,
                    pg_catalog.pg_get_constraintdef(con.oid, true)
             FROM pg_catalog.pg_constraint con
             JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = ANY($2::text[])
               AND con.contype <> 'n'
             ORDER BY c.relname, con.conname
             """,
             [@prefix, @required_tables]
           ),
         {:ok, indexes} <-
           query_rows(
             repo,
             """
             SELECT table_class.relname, index_class.relname,
                    idx.indisvalid, idx.indisready, idx.indisunique, idx.indisprimary,
                    pg_catalog.pg_get_indexdef(index_class.oid)
             FROM pg_catalog.pg_index idx
             JOIN pg_catalog.pg_class index_class ON index_class.oid = idx.indexrelid
             JOIN pg_catalog.pg_class table_class ON table_class.oid = idx.indrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = table_class.relnamespace
             WHERE n.nspname = $1 AND table_class.relname = ANY($2::text[])
             ORDER BY table_class.relname, index_class.relname
             """,
             [@prefix, @required_tables]
           ) do
      fingerprint =
        %{columns: columns, constraints: constraints, indexes: indexes}
        |> :erlang.term_to_binary()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, fingerprint}
    end
  end

  defp projection_health(repo) do
    case SQL.query(
           repo,
           """
           SELECT cursor.last_publication_id,
                  publication.last_publication_id,
                  EXISTS (
                    SELECT 1 FROM favn_control.projection_failures failure
                    WHERE failure.projector_name = 'control_plane_v1' AND failure.shard_id = 0
                  )
           FROM favn_control.outbox_publication_state publication
           LEFT JOIN favn_control.projection_cursors cursor
             ON cursor.projector_name = 'control_plane_v1' AND cursor.shard_id = 0
           WHERE publication.singleton_id = 1
           """,
           []
         ) do
      {:ok, %{rows: [[cursor, publication, blocked?]]}} ->
        {:ok,
         %{
           ready?: is_integer(cursor) and not blocked?,
           cursor_present?: is_integer(cursor),
           blocked?: blocked?,
           last_publication_id: cursor,
           published_through: publication,
           lag: if(is_integer(cursor), do: max(publication - cursor, 0), else: nil)
         }}

      {:ok, %{rows: []}} ->
        {:ok,
         %{
           ready?: false,
           cursor_present?: false,
           blocked?: false,
           last_publication_id: nil,
           published_through: nil,
           lag: nil
         }}

      {:error, %Postgrex.Error{postgres: %{code: code}}}
      when code in [:undefined_table, :invalid_schema_name] ->
        {:ok,
         %{
           ready?: false,
           cursor_present?: false,
           blocked?: false,
           last_publication_id: nil,
           published_through: nil,
           lag: nil
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp query_rows(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: rows}} ->
        {:ok, rows}

      {:error, %Postgrex.Error{postgres: %{code: code}}}
      when code in [:undefined_table, :invalid_schema_name] ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp status(18, [], [], [], [], [], [], [], true, true, true), do: :ready

  defp status(
         major,
         _tables,
         _indexes,
         _columns,
         _unexpected,
         _constraints,
         _versions,
         _future,
         _definition,
         _projection,
         _runtime_role
       )
       when major != 18,
       do: :incompatible

  defp status(
         _major,
         missing_tables,
         _indexes,
         _columns,
         _unexpected,
         _constraints,
         missing_versions,
         [],
         _definition,
         _projection,
         _runtime_role
       )
       when missing_tables == @required_tables and missing_versions == @expected_versions,
       do: :empty_database

  defp status(
         _major,
         _tables,
         _indexes,
         _columns,
         _unexpected,
         _constraints,
         _versions,
         future,
         _definition,
         _projection,
         _runtime_role
       )
       when future != [],
       do: :incompatible

  defp status(
         _major,
         _tables,
         _indexes,
         _columns,
         _unexpected,
         _constraints,
         _versions,
         _future,
         _definition,
         _projection,
         _runtime_role
       ),
       do: :upgrade_required
end
