CREATE TABLE IF NOT EXISTS llm_agent_runs (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES llm_agents(id) ON DELETE CASCADE,
    run_status TEXT NOT NULL DEFAULT 'running',
    workflow_id TEXT,
    conversation_id TEXT,
    response_text TEXT,
    error TEXT,
    duration_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);
