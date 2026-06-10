CREATE TABLE IF NOT EXISTS trigger_events (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    idempotency_key TEXT,
    correlation_id TEXT,
    matched_agent_id TEXT,
    matched_run_id TEXT,
    state TEXT NOT NULL DEFAULT 'received',
    received_at TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at TEXT
);
