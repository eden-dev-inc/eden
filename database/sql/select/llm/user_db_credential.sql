SELECT
    id,
    user_uuid,
    organization_uuid,
    endpoint_uuid,
    db_username,
    db_password_encrypted,
    auth_method,
    created_at,
    updated_at
FROM user_db_credentials
WHERE user_uuid = $1 AND endpoint_uuid = $2;
