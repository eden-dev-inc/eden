SELECT COALESCE(MAX(version), 0) + 1 AS next_version
FROM els_policy_versions
WHERE policy_uuid = $1;
