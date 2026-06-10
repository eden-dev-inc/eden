INSERT INTO user_db_credentials (
    id,
    user_uuid,
    organization_uuid,
    endpoint_uuid,
    db_username,
    db_password_encrypted,
    auth_method
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
ON CONFLICT (user_uuid, endpoint_uuid)
DO UPDATE SET
    organization_uuid = EXCLUDED.organization_uuid,
    db_username = EXCLUDED.db_username,
    db_password_encrypted = EXCLUDED.db_password_encrypted,
    auth_method = EXCLUDED.auth_method,
    updated_at = NOW()
RETURNING
    id,
    user_uuid,
    organization_uuid,
    endpoint_uuid,
    db_username,
    db_password_encrypted,
    auth_method,
    created_at,
    updated_at;
