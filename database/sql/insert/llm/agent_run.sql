INSERT INTO llm_agent_runs (id, agent_id, run_status, workflow_id, conversation_id)
SELECT $1, $2, $3, $4, $5
WHERE NOT EXISTS (
    SELECT 1 FROM llm_agent_runs WHERE id = $1
)
