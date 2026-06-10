SELECT
    u.uuid,
    u.username,
    u.organization_uuid,
    u.password,
    u.description,
    u.email,
    u.display_name,
    u.bio,
    u.created_by,
    u.updated_by,
    u.created_at,
    u.updated_at
FROM users u
WHERE username = $1
  AND organization_uuid = $2;
