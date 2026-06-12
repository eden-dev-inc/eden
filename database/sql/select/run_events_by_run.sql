SELECT id, run_id, event_type, payload, tokens_used, trace_id, created_at
FROM run_events
WHERE run_id = $1
ORDER BY created_at DESC
LIMIT $2
