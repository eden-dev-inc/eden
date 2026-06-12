SELECT key_uuid, org_uuid, endpoint_uuid, wrapped_key, wrapping_org, version, is_active, created_at, rotated_at
FROM encryption_keys
WHERE org_uuid = $1
  AND endpoint_uuid = $2
  AND is_active = TRUE
ORDER BY version DESC
LIMIT 1;
