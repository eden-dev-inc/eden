CREATE TABLE IF NOT EXISTS organizations (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    description TEXT,
    rate_limit_settings TEXT,
    created_at TEXT,
    updated_at TEXT
);
