CREATE TABLE IF NOT EXISTS jwt_blacklist
(
    blacklist_key TEXT PRIMARY KEY,
    expires_at_ms INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
