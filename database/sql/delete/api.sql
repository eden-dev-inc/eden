WITH deleted_organization_apis AS (
    DELETE FROM organization_apis
    WHERE api_uuid = $1
    RETURNING organization_uuid
),
deleted_api AS (
    DELETE FROM apis
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_apis), '[]'::json) as organization_uuids;
