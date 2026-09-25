SELECT jsonb_build_object('candidate', (
  SELECT jsonb_build_object('task_id', t.task_id, 'run_id', t.run_id, 'target_id', c.target_id,
    'generation_id', c.target_generation_id, 'generation_status', g.status,
    'assignment_generation', o.assignment_generation, 'receipt_inserted_at', o.inserted_at,
    'materialization_id', m.materialization_id)
  FROM favn_control.runner_tasks t
  JOIN favn_control.runner_task_outcomes o USING(workspace_id, task_id)
  JOIN favn_control.materialization_claims c ON c.workspace_id = t.workspace_id
    AND c.claim_key = t.write_claim_key
  JOIN favn_control.asset_target_generations g ON g.workspace_id = c.workspace_id
    AND g.target_id = c.target_id AND g.target_generation_id = c.target_generation_id
  LEFT JOIN favn_control.materializations m ON m.workspace_id = c.workspace_id AND m.claim_key = c.claim_key
  WHERE t.run_id = :run_id AND t.task_kind = 'asset_attempt' AND t.status = 'succeeded'
    AND o.assignment_generation = t.assignment_generation AND o.error IS NULL AND g.status = :generation_state AND :phase_predicate
    AND NOT EXISTS (SELECT 1 FROM favn_control.runner_tasks marker
      WHERE marker.workspace_id = c.workspace_id AND marker.write_target_id = c.target_id
        AND marker.task_kind = 'generation_marker_initialize')
  ORDER BY o.inserted_at LIMIT 1
));
