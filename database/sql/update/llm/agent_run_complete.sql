UPDATE llm_agent_runs
SET run_status = $2,
    response_text = $3,
    error = $4,
    duration_ms = $5,
    completed_at = NOW()
WHERE id = $1
