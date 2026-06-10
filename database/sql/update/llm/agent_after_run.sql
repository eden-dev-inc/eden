UPDATE llm_agents
SET last_run_at = NOW(),
    next_run_at = $2,
    consecutive_failures = $3,
    status = $4,
    updated_at = NOW()
WHERE id = $1
