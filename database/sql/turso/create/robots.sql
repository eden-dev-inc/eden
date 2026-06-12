CREATE TABLE IF NOT EXISTS robots (
    uuid TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    api_key TEXT NOT NULL,
    description TEXT,
    ttl INTEGER,
    expires_at TEXT,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (username, organization_uuid)
);
