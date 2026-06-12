SELECT org_uuid, provider, key_ref, key_version, created_at, rotated_at
FROM org_key_refs
WHERE org_uuid = $1;
