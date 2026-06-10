SELECT
    a.org_uuid,
    a.endpoint_uuid,
    a.user_uuid,
    COALESCE(a.strategy_snapshot, p.strategy) AS strategy,
    COALESCE(a.config_snapshot, p.config) AS config
FROM els_policy_assignments a
LEFT JOIN els_policies p ON a.policy_uuid = p.uuid
ORDER BY a.org_uuid ASC, a.endpoint_uuid ASC, a.user_uuid ASC
LIMIT $1 OFFSET $2;
