CREATE TABLE IF NOT EXISTS els_policies (
    uuid TEXT PRIMARY KEY,
    org_uuid TEXT NOT NULL,
    endpoint_uuid TEXT NOT NULL,
    name TEXT NOT NULL,
    strategy TEXT NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(endpoint_uuid, name)
);
CREATE INDEX IF NOT EXISTS idx_els_policies_org ON els_policies (org_uuid);
