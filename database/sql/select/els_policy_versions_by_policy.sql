SELECT policy_uuid, version, strategy, config, status, created_by, created_at
FROM els_policy_versions
WHERE policy_uuid = $1
ORDER BY version DESC
LIMIT $2 OFFSET $3;
