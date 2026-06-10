UPDATE execution_runs
SET state = $2,
    response_text = $3,
    error = $4,
    duration_ms = $5,
    updated_at = NOW(),
    completed_at = NOW()
WHERE id = $1
