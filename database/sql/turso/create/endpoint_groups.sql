CREATE TABLE IF NOT EXISTS endpoint_groups (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    description TEXT,
    ep_kind TEXT NOT NULL,
    default_endpoint TEXT,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT
);
