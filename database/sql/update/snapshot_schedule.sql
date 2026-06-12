UPDATE snapshots
SET last_run_at = $2, next_run_at = $3, updated_at = NOW()
WHERE uuid = $1;
