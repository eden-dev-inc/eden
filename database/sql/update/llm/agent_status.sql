UPDATE llm_agents
SET status = $2,
    updated_at = NOW()
WHERE id = $1
