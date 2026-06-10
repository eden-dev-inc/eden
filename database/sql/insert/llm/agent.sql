INSERT INTO llm_agents (id, version, name, description, prompt, cron_expression, status, scope, overlap_policy,
    endpoint_uuid, organization_uuid, created_by, robot_uuid, skill_ids, tool_endpoint_uuids, orchestrate,
    max_consecutive_failures, next_run_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
