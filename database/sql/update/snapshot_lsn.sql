UPDATE snapshots
SET last_lsn = $2, updated_at = NOW()
WHERE uuid = $1
