WITH deleted_organization_robots AS (
    DELETE FROM organization_robots
    WHERE robot_uuid = $1
    RETURNING organization_uuid
),
deleted_robot AS (
    DELETE FROM robots
    WHERE uuid = $1
)
SELECT
    COALESCE((SELECT json_agg(organization_uuid) FROM deleted_organization_robots), '[]'::json) as organization_uuids;
