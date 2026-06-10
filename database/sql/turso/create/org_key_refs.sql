CREATE TABLE IF NOT EXISTS org_key_refs (
    org_uuid TEXT PRIMARY KEY REFERENCES organizations(uuid),
    provider TEXT NOT NULL DEFAULT 'env',
    key_ref TEXT NOT NULL,
    key_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    rotated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
