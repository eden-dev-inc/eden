CREATE TABLE IF NOT EXISTS templates (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    template TEXT,
    description TEXT,
    llm_recommendation TEXT,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    created_at TEXT,
    updated_at TEXT
);
