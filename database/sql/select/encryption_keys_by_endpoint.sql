SELECT wrapped_key
FROM encryption_keys
WHERE org_uuid = $1
  AND endpoint_uuid = $2
ORDER BY is_active DESC, version DESC;
