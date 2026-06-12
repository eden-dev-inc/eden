UPDATE llm_agents
SET version = $2,
    name = $3,
    description = $4,
    prompt = $5,
    cron_expression = $6,
    scope = $7,
    overlap_policy = $8,
    endpoint_uuid = $9,
    robot_uuid = $10,
    skill_ids = $11,
    tool_endpoint_uuids = $12,
    orchestrate = $13,
    max_consecutive_failures = $14,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING id, version, name, description, prompt, cron_expression, status, scope, overlap_policy,
          endpoint_uuid, organization_uuid, created_by, robot_uuid, skill_ids, tool_endpoint_uuids,
          orchestrate, max_consecutive_failures, consecutive_failures, last_run_at, next_run_at,
          created_at, updated_at
