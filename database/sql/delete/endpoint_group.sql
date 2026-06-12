WITH deleted_organization_endpoint_groups AS (
    DELETE FROM organization_endpoint_groups
    WHERE endpoint_group_uuid = $1
    RETURNING organization_uuid
),
deleted_endpoint_group_members AS (
    DELETE FROM endpoint_group_members
    WHERE endpoint_group_uuid = $1
),
deleted_endpoint_group AS (
    DELETE FROM endpoint_groups
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_endpoint_groups), '[]'::json) as organization_uuids;
