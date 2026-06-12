UPDATE auths
SET auth = $2, updated_at = $3
WHERE uuid = $1