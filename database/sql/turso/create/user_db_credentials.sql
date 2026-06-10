CREATE TABLE IF NOT EXISTS user_db_credentials (
    id TEXT PRIMARY KEY,
    user_uuid TEXT NOT NULL,
    organization_uuid TEXT NOT NULL,
    endpoint_uuid TEXT NOT NULL,
    db_username TEXT NOT NULL,
    db_password_encrypted BLOB NOT NULL,
    auth_method TEXT NOT NULL DEFAULT 'password',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (user_uuid, endpoint_uuid)
);
