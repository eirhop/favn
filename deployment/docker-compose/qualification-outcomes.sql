\set ON_ERROR_STOP on

WITH qualification_runs AS (
  SELECT run_id, status, inserted_at, terminal_at
  FROM favn_control.runs
  WHERE workspace_id = 'elastic-simulation'
    AND inserted_at >= :'qualification_started_at'::timestamptz
),
terminal_failures AS (
  SELECT DISTINCT ON (run_id)
    run_id,
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
  ORDER BY run_id, sequence DESC
)
SELECT jsonb_agg(
  jsonb_build_object(
    'run_id', qualification_runs.run_id,
    'run_status', qualification_runs.status,
    'run_inserted_at', qualification_runs.inserted_at,
    'run_terminal_at', qualification_runs.terminal_at,
    'failure_reason', terminal_failures.reason,
    'runner_task_id', runner_tasks.task_id,
    'runner_task_status', runner_tasks.status,
    'runner_task_terminal_at', runner_tasks.terminal_at
  )
  ORDER BY qualification_runs.inserted_at
)
FROM qualification_runs
LEFT JOIN terminal_failures
  ON terminal_failures.run_id = qualification_runs.run_id
LEFT JOIN favn_control.runner_tasks
  ON runner_tasks.workspace_id = 'elastic-simulation'
  AND runner_tasks.run_id = qualification_runs.run_id;
