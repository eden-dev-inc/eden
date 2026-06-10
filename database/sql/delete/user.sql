-- Option 1: Enhanced original approach with admin check
WITH deleted_organization_users AS (
    DELETE FROM organization_users
    WHERE user_uuid = $1
    RETURNING organization_uuid
),
deleted_user AS (
    DELETE FROM users
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_users), '[]'::json) as organization_uuids;
