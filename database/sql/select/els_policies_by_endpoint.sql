SELECT uuid, endpoint_uuid, name, strategy, config, created_at, updated_at
FROM els_policies
WHERE endpoint_uuid = $1 AND org_uuid = $2
ORDER BY created_at DESC, uuid DESC
LIMIT $3 OFFSET $4;
