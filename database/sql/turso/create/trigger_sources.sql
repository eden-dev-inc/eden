CREATE TABLE IF NOT EXISTS trigger_sources (
    id TEXT PRIMARY KEY,
    organization_uuid TEXT NOT NULL,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    hmac_secret TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
