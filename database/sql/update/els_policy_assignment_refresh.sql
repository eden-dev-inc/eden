UPDATE els_policy_assignments
SET strategy_snapshot = $4,
    config_snapshot = $5,
    updated_at = NOW()
WHERE endpoint_uuid = $1
  AND user_uuid = $2
  AND org_uuid = $3
  AND mode = 'copy';
