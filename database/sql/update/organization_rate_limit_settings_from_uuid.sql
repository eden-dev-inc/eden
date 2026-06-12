UPDATE organizations
SET rate_limit_settings = $2, updated_at = $3
WHERE uuid = $1;
