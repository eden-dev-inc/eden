SELECT
    r.uuid,
    r.username,
    r.organization_uuid,
    r.api_key,
    r.description,
    r.ttl,
    r.expires_at,
    r.created_by,
    r.updated_by,
    r.created_at,
    r.updated_at
FROM robots r
WHERE username = $1 AND organization_uuid = $2;
