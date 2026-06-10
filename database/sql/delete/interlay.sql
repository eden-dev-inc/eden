WITH deleted_organization_interlays AS (
    DELETE FROM organization_interlays
    WHERE interlay_uuid = $1
    RETURNING organization_uuid
),
deleted_interlay AS (
    DELETE FROM interlays
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_interlays), '[]'::json) as organization_uuids;
