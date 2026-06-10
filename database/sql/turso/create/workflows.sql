CREATE TABLE IF NOT EXISTS workflows (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    dag TEXT,
    description TEXT,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT
);
