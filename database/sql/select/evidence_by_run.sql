SELECT id, run_id, step_index, kind, payload, source, timestamp_ms, created_at
FROM evidence_records
WHERE run_id = $1
ORDER BY step_index, timestamp_ms
