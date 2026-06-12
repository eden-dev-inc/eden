UPDATE snapshots
SET status = $2, updated_at = NOW()
WHERE uuid = $1;
