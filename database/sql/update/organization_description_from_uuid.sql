UPDATE organizations
SET description = $2, updated_at = $3
WHERE uuid = $1