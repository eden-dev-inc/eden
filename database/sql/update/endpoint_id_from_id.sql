UPDATE endpoints
SET id = $2, updated_at = $3, updated_by = COALESCE($4, updated_by)
WHERE id = $1