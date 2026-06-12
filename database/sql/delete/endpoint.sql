WITH deleted_endpoint_data AS (
    SELECT uuid
    FROM endpoints
    WHERE uuid = $1
),
deleted_cascade_data AS (
    -- Get all related data in one query
    SELECT
        array_remove(array_agg(DISTINCT a.uuid), NULL) as auth_uuids,
        array_remove(array_agg(DISTINCT ne.eden_node_uuid), NULL) as eden_node_uuids,
        array_remove(array_agg(DISTINCT oe.organization_uuid), NULL) as organization_uuids
    FROM deleted_endpoint_data e
    LEFT JOIN auths a ON a.endpoint_uuid = e.uuid
    LEFT JOIN eden_node_endpoints ne ON ne.endpoint_uuid = e.uuid
    LEFT JOIN organization_endpoints oe ON oe.endpoint_uuid = e.uuid
),
-- Delete everything in parallel using the collected IDs
deleted_eden_node_ref AS (
    DELETE FROM eden_node_endpoints
    WHERE endpoint_uuid = $1
),
deleted_organization_ref AS (
    DELETE FROM organization_endpoints
    WHERE endpoint_uuid = $1
),
deleted_auth_ref AS (
    DELETE FROM auths
    WHERE endpoint_uuid = $1
),
deleted_endpoint_group_ref AS (
    DELETE FROM endpoint_group_members
    WHERE endpoint_uuid = $1
),
deleted_endpoint AS (
    DELETE FROM endpoints
    WHERE uuid = $1
)
SELECT
    COALESCE(auth_uuids, '{}'::uuid[]) AS auth_uuids,
    COALESCE(eden_node_uuids, '{}'::uuid[]) AS eden_node_uuids,
    COALESCE(organization_uuids, '{}'::uuid[]) AS organization_uuids
FROM deleted_cascade_data;
