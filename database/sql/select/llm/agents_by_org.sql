SELECT id, version, name, description, prompt, cron_expression, status, scope, overlap_policy,
    endpoint_uuid, organization_uuid, created_by, robot_uuid, skill_ids, tool_endpoint_uuids, orchestrate,
    max_consecutive_failures, consecutive_failures, last_run_at, next_run_at, created_at, updated_at
FROM llm_agents
WHERE organization_uuid = $1
ORDER BY created_at DESC
