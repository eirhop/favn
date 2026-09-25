WITH tasks AS (
  SELECT task_kind, status, count(*) AS count
  FROM favn_control.runner_tasks GROUP BY task_kind, status
), generations AS (
  SELECT status, count(*) AS count FROM favn_control.asset_target_generations GROUP BY status
), claims AS (
  SELECT status, effect_state, count(*) AS count
  FROM favn_control.materialization_claims GROUP BY status, effect_state
)
SELECT jsonb_build_object(
  'database_time', clock_timestamp(),
  'runs', COALESCE((SELECT jsonb_agg(jsonb_build_object('run_id', run_id, 'status', status,
      'inserted_at', inserted_at, 'terminal_at', terminal_at)) FROM favn_control.runs), '[]'::jsonb),
  'tasks', COALESCE((SELECT jsonb_agg(to_jsonb(tasks)) FROM tasks), '[]'::jsonb),
  'generations', COALESCE((SELECT jsonb_agg(to_jsonb(generations)) FROM generations), '[]'::jsonb),
  'claims', COALESCE((SELECT jsonb_agg(to_jsonb(claims)) FROM claims), '[]'::jsonb),
  'materializations', (SELECT count(*) FROM favn_control.materializations),
  'open_session_rows', COALESCE((SELECT jsonb_agg(jsonb_build_object('runner_instance_id', runner_instance_id,
      'session_generation', session_generation, 'release_id', required_runner_release_id,
      'registered_at', registered_at)) FROM favn_control.runner_sessions WHERE ended_at IS NULL), '[]'::jsonb),
  'assigned_runner_count', (SELECT count(DISTINCT assigned_runner_instance_id) FROM favn_control.runner_tasks
      WHERE status IN ('assigned','preparing','running','cancelling')),
  'successful_asset_receipts', (SELECT count(*) FROM favn_control.runner_task_outcomes o
      JOIN favn_control.runner_tasks t USING(workspace_id, task_id)
      WHERE t.task_kind = 'asset_attempt' AND t.status = 'succeeded' AND o.assignment_generation = t.assignment_generation AND o.error IS NULL)
);
