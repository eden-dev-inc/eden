CREATE TABLE IF NOT EXISTS encryption_keys (
    key_uuid TEXT PRIMARY KEY,
    org_uuid TEXT NOT NULL REFERENCES organizations(uuid),
    endpoint_uuid TEXT NOT NULL,
    wrapped_key BLOB NOT NULL,
    wrapping_org TEXT NOT NULL REFERENCES org_key_refs(org_uuid),
    version INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    rotated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (org_uuid, endpoint_uuid, version)
);
