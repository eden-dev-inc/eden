SELECT COUNT(*)::BIGINT AS total
FROM els_policy_versions
WHERE policy_uuid = $1;
