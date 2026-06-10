INSERT INTO organization_endpoints (organization_uuid, endpoint_uuid)
VALUES ($1, $2)
    ON CONFLICT (organization_uuid, endpoint_uuid) DO NOTHING;
