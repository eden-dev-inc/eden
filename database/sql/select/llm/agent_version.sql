SELECT id, agent_id, version, prompt, cron_expression, scope, skill_ids, tool_endpoint_uuids,
       orchestrate, created_at, created_by
FROM llm_agent_versions
WHERE agent_id = $1
  AND version = $2
LIMIT 1
