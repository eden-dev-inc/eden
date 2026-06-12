UPDATE users
SET password = $2, updated_at = $3, updated_by = COALESCE($4, updated_by)
WHERE username = $1
