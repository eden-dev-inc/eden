SELECT
    a.endpoint_uuid,
    a.user_uuid,
    a.policy_uuid,
    a.mode,
    p.name AS policy_name,
    COALESCE(a.strategy_snapshot, p.strategy) AS strategy,
    COALESCE(a.config_snapshot, p.config) AS config
FROM els_policy_assignments a
LEFT JOIN els_policies p ON a.policy_uuid = p.uuid
WHERE a.endpoint_uuid = $1 AND a.org_uuid = $2
ORDER BY a.created_at DESC, a.user_uuid ASC
LIMIT $3 OFFSET $4;
