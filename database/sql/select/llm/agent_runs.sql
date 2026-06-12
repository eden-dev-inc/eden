SELECT id, agent_id, run_status, workflow_id, conversation_id, response_text, error, duration_ms, created_at, completed_at
FROM llm_agent_runs
WHERE agent_id = $1
ORDER BY created_at DESC
LIMIT $2
