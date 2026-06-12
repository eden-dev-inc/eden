SELECT
    id,
    agent_id,
    CASE
        WHEN state IN ('planning', 'queued', 'executing') THEN 'running'
        WHEN state = 'awaiting_approval' THEN 'awaiting_approval'
        WHEN state = 'completed' THEN 'completed'
        WHEN state = 'rejected' THEN 'rejected'
        ELSE 'failed'
    END AS run_status,
    id AS workflow_id,
    conversation_id,
    response_text,
    error,
    duration_ms,
    created_at,
    completed_at
FROM execution_runs
WHERE agent_id = $1
ORDER BY created_at DESC
LIMIT $2
