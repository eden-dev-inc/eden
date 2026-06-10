CREATE TABLE IF NOT EXISTS run_events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    tokens_used INTEGER,
    cost_usd REAL,
    trace_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
