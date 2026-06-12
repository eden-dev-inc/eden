UPDATE robots
SET ttl = $2, expires_at = $3, updated_at = $4, updated_by = COALESCE($5, updated_by)
WHERE uuid = $1;
