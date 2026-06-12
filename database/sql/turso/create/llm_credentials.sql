CREATE TABLE IF NOT EXISTS llm_credentials (
    id TEXT PRIMARY KEY,
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    label TEXT,
    description TEXT,
    base_url TEXT,
    api_key TEXT NOT NULL,
    deleted_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
