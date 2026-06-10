SELECT id, agent_id, version, prompt, cron_expression, scope, skill_ids, tool_endpoint_uuids,
       orchestrate, created_at, created_by
FROM llm_agent_versions
WHERE agent_id = $1
ORDER BY version DESC
LIMIT $2
