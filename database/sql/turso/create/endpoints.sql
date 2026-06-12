CREATE TABLE IF NOT EXISTS endpoints (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    kind TEXT,
    config BLOB,
    routing TEXT,
    description TEXT,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT
);
