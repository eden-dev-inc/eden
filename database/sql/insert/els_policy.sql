INSERT INTO els_policies (uuid, org_uuid, endpoint_uuid, name, strategy, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
ON CONFLICT (endpoint_uuid, name)
DO UPDATE SET
    strategy = EXCLUDED.strategy,
    config = EXCLUDED.config,
    updated_at = NOW()
RETURNING uuid;
