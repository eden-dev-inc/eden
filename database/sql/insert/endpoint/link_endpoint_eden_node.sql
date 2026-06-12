INSERT INTO eden_node_endpoints (eden_node_uuid, endpoint_uuid, created_at, updated_at)
VALUES ($1, $2, $3, $4)
    ON CONFLICT (eden_node_uuid, endpoint_uuid) DO UPDATE
                                                       SET updated_at = EXCLUDED.updated_at;