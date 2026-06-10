INSERT INTO els_policy_assignments (org_uuid, endpoint_uuid, user_uuid, policy_uuid, mode, strategy_snapshot, config_snapshot, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
ON CONFLICT (endpoint_uuid, user_uuid)
DO UPDATE SET policy_uuid = $4, mode = $5, strategy_snapshot = $6, config_snapshot = $7, updated_at = NOW();
