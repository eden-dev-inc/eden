CREATE TABLE IF NOT EXISTS users (
    uuid TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    password TEXT,
    description TEXT,
    email TEXT,
    display_name TEXT,
    bio TEXT DEFAULT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (username, organization_uuid)
);
