CREATE TABLE IF NOT EXISTS user_db_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_uuid UUID NOT NULL,
    organization_uuid UUID NOT NULL,
    endpoint_uuid UUID NOT NULL,
    db_username TEXT NOT NULL,
    db_password_encrypted BYTEA NOT NULL,
    auth_method TEXT NOT NULL DEFAULT 'password',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_uuid, endpoint_uuid)
);
