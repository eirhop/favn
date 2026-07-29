\set ON_ERROR_STOP on

WITH database_stats AS (
  SELECT
    numbackends,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    conflicts,
    temp_files,
    temp_bytes,
    deadlocks
  FROM pg_stat_database
  WHERE datname = current_database()
),
activity AS (
  SELECT
    count(*) AS connections,
    count(*) FILTER (WHERE wait_event IS NOT NULL) AS waiting_connections
  FROM pg_stat_activity
  WHERE datname = current_database()
),
locks AS (
  SELECT count(*) FILTER (WHERE NOT granted) AS waiting_locks
  FROM pg_locks
),
table_stats AS (
  SELECT
    coalesce(sum(n_live_tup), 0) AS live_rows,
    coalesce(sum(n_dead_tup), 0) AS dead_rows,
    coalesce(sum(vacuum_count), 0) AS manual_vacuums,
    coalesce(sum(autovacuum_count), 0) AS autovacuums,
    coalesce(sum(analyze_count), 0) AS manual_analyzes,
    coalesce(sum(autoanalyze_count), 0) AS autoanalyzes
  FROM pg_stat_user_tables
  WHERE schemaname = 'favn_control'
),
submission_statuses AS (
  SELECT coalesce(jsonb_object_agg(status, total), '{}'::jsonb) AS value
  FROM (
    SELECT status, count(*) AS total
    FROM favn_control.run_submissions
    WHERE workspace_id = 'elastic-simulation'
      AND inserted_at >= :'qualification_started_at'::timestamptz
    GROUP BY status
  ) grouped
),
run_statuses AS (
  SELECT coalesce(jsonb_object_agg(status, total), '{}'::jsonb) AS value
  FROM (
    SELECT status, count(*) AS total
    FROM favn_control.runs
    WHERE workspace_id = 'elastic-simulation'
      AND inserted_at >= :'qualification_started_at'::timestamptz
    GROUP BY status
  ) grouped
),
run_terminal_state AS (
  SELECT
    count(*) FILTER (
      WHERE status IN ('ok', 'partial', 'error', 'cancelled', 'timed_out')
    ) AS terminal,
    count(*) FILTER (
      WHERE status NOT IN ('ok', 'partial', 'error', 'cancelled', 'timed_out')
    ) AS non_terminal
  FROM favn_control.runs
  WHERE workspace_id = 'elastic-simulation'
    AND inserted_at >= :'qualification_started_at'::timestamptz
),
run_failure_reasons AS (
  SELECT coalesce(jsonb_object_agg(reason, total), '{}'::jsonb) AS value
  FROM (
    SELECT reason, count(*) AS total
    FROM (
      SELECT
        CASE
          WHEN jsonb_typeof(event -> 'data' -> 'error') = 'object'
            AND event -> 'data' -> 'error' ->> 'outcome' = 'unknown'
            THEN 'runner_outcome_unknown'
          WHEN jsonb_typeof(event -> 'data' -> 'error') = 'array'
            AND event -> 'data' -> 'error' ->> 0 =
              'non_reusable_materialization_claim_succeeded'
            THEN 'non_reusable_materialization_claim_succeeded'
          ELSE 'other'
        END AS reason
      FROM favn_control.run_events
      WHERE workspace_id = 'elastic-simulation'
        AND inserted_at >= :'qualification_started_at'::timestamptz
        AND event_type = 'run_failed'
    ) classified
    GROUP BY reason
  ) grouped
),
runner_task_statuses AS (
  SELECT coalesce(jsonb_object_agg(status, total), '{}'::jsonb) AS value
  FROM (
    SELECT status, count(*) AS total
    FROM favn_control.runner_tasks
    WHERE workspace_id = 'elastic-simulation'
      AND inserted_at >= :'qualification_started_at'::timestamptz
    GROUP BY status
  ) grouped
),
runner_task_terminal_state AS (
  SELECT
    count(*) FILTER (
      WHERE status IN ('succeeded', 'failed', 'cancelled', 'unknown')
    ) AS terminal,
    count(*) FILTER (
      WHERE status NOT IN ('succeeded', 'failed', 'cancelled', 'unknown')
    ) AS non_terminal
  FROM favn_control.runner_tasks
  WHERE workspace_id = 'elastic-simulation'
    AND inserted_at >= :'qualification_started_at'::timestamptz
),
outbox AS (
  SELECT
    count(*) FILTER (WHERE publication_id IS NULL) AS unsequenced,
    count(*) FILTER (WHERE published_at IS NULL) AS unpublished,
    coalesce(max(publication_id), 0) AS last_publication_id
  FROM favn_control.outbox_events
),
projection AS (
  SELECT
    coalesce(min(last_publication_id), 0) AS last_publication_id,
    count(*) FILTER (WHERE claim_expires_at > clock_timestamp()) AS active_claims
  FROM favn_control.projection_cursors
),
projection_failures AS (
  SELECT count(*) AS total
  FROM favn_control.projection_failures
)
SELECT jsonb_build_object(
  'observed_at', clock_timestamp(),
  'database_size_bytes', pg_database_size(current_database()),
  'connections', activity.connections,
  'waiting_connections', activity.waiting_connections,
  'waiting_locks', locks.waiting_locks,
  'xact_commit', database_stats.xact_commit,
  'xact_rollback', database_stats.xact_rollback,
  'blocks_read', database_stats.blks_read,
  'blocks_hit', database_stats.blks_hit,
  'tuples_returned', database_stats.tup_returned,
  'tuples_fetched', database_stats.tup_fetched,
  'tuples_inserted', database_stats.tup_inserted,
  'tuples_updated', database_stats.tup_updated,
  'tuples_deleted', database_stats.tup_deleted,
  'conflicts', database_stats.conflicts,
  'temp_files', database_stats.temp_files,
  'temp_bytes', database_stats.temp_bytes,
  'deadlocks', database_stats.deadlocks,
  'live_rows', table_stats.live_rows,
  'dead_rows', table_stats.dead_rows,
  'manual_vacuums', table_stats.manual_vacuums,
  'autovacuums', table_stats.autovacuums,
  'manual_analyzes', table_stats.manual_analyzes,
  'autoanalyzes', table_stats.autoanalyzes,
  'submission_statuses', submission_statuses.value,
  'run_statuses', run_statuses.value,
  'run_terminal', run_terminal_state.terminal,
  'run_non_terminal', run_terminal_state.non_terminal,
  'run_failure_reasons', run_failure_reasons.value,
  'runner_task_statuses', runner_task_statuses.value,
  'runner_task_terminal', runner_task_terminal_state.terminal,
  'runner_task_non_terminal', runner_task_terminal_state.non_terminal,
  'outbox_unsequenced', outbox.unsequenced,
  'outbox_unpublished', outbox.unpublished,
  'outbox_last_publication_id', outbox.last_publication_id,
  'projection_last_publication_id', projection.last_publication_id,
  'projection_lag', greatest(outbox.last_publication_id - projection.last_publication_id, 0),
  'projection_active_claims', projection.active_claims,
  'projection_failures', projection_failures.total
)
FROM database_stats, activity, locks, table_stats, submission_statuses,
     run_statuses, run_terminal_state, run_failure_reasons,
     runner_task_statuses, runner_task_terminal_state, outbox, projection,
     projection_failures;
