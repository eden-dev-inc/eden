SELECT endpoint_uuid, user_uuid, policy_uuid, mode, strategy_snapshot, config_snapshot
FROM els_policy_assignments
WHERE policy_uuid = $1 AND mode = 'sync';
