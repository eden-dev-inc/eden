-- Create the new user with entry in organization_users
WITH user_insert AS (
    INSERT INTO users (uuid, username, organization_uuid, "password", "description", email, display_name, bio, created_by, updated_by, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
)
INSERT INTO organization_users (organization_uuid, user_uuid)
VALUES ($3, $1)
