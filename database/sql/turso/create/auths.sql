CREATE TABLE IF NOT EXISTS auths (
    id TEXT UNIQUE NOT NULL,
    uuid TEXT PRIMARY KEY,
    auth TEXT,
    endpoint_uuid TEXT UNIQUE,
    created_at TEXT,
    updated_at TEXT,
    FOREIGN KEY (endpoint_uuid) REFERENCES endpoints(uuid) ON DELETE CASCADE
);
