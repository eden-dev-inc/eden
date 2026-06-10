SELECT policy_uuid, version, strategy, config, status, created_by, created_at
FROM els_policy_versions
WHERE policy_uuid = $1 AND version = $2;
