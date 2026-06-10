WITH deleted_node_endpoints AS (
    DELETE FROM eden_node_endpoints
    WHERE eden_node_uuid = $1
    RETURNING endpoint_uuid
),
deleted_org_nodes AS (
    DELETE FROM organization_eden_nodes
    WHERE eden_node_uuid = $1
    RETURNING organization_uuid
),
deleted_node AS (
    DELETE FROM eden_nodes
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(endpoint_uuid) FROM deleted_node_endpoints), '[]'::json) as endpoint_uuids,
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_org_nodes), '[]'::json) as organization_uuids;
