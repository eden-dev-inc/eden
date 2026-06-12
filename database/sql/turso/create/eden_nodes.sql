CREATE TABLE IF NOT EXISTS eden_nodes (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    info TEXT,
    description TEXT,
    created_at TEXT,
    updated_at TEXT
);
