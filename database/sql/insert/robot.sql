-- Create the new robot with entry in organization_robots
WITH robot_insert AS (
    INSERT INTO robots (uuid, username, organization_uuid, api_key, description, ttl, expires_at, created_by, updated_by, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
)
INSERT INTO organization_robots (organization_uuid, robot_uuid)
VALUES ($3, $1)
