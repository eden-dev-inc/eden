UPDATE execution_runs
SET checkpoint = $2,
    state = $3,
    updated_at = NOW()
WHERE id = $1
