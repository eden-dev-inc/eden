UPDATE els_policies
SET strategy = $3, config = $4, updated_at = NOW()
WHERE uuid = $1 AND endpoint_uuid = $2 AND org_uuid = $5;
