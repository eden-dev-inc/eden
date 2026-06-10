INSERT INTO els_policy_assignments (
    org_uuid,
    endpoint_uuid,
    user_uuid,
    policy_uuid,
    mode,
    strategy_snapshot,
    config_snapshot,
    created_at,
    updated_at
)
SELECT
    $1,
    $2,
    user_uuid,
    $4,
    $5,
    $6,
    $7,
    NOW(),
    NOW()
FROM unnest($3::uuid[]) AS input(user_uuid)
ON CONFLICT (endpoint_uuid, user_uuid)
DO UPDATE SET
    policy_uuid = EXCLUDED.policy_uuid,
    mode = EXCLUDED.mode,
    strategy_snapshot = EXCLUDED.strategy_snapshot,
    config_snapshot = EXCLUDED.config_snapshot,
    updated_at = NOW();
