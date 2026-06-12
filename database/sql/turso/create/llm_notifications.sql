CREATE TABLE IF NOT EXISTS llm_notifications (
    id TEXT PRIMARY KEY,
    user_uuid TEXT NOT NULL,
    organization_uuid TEXT NOT NULL,
    agent_id TEXT REFERENCES llm_agents(id) ON DELETE SET NULL,
    run_id TEXT REFERENCES llm_agent_runs(id) ON DELETE SET NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    read INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
