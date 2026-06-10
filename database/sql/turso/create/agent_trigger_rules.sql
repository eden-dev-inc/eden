CREATE TABLE IF NOT EXISTS agent_trigger_rules (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    event_type_filter TEXT,
    payload_filter TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
