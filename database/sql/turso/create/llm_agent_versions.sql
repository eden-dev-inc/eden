CREATE TABLE IF NOT EXISTS llm_agent_versions (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    prompt TEXT NOT NULL,
    cron_expression TEXT NOT NULL,
    scope TEXT NOT NULL,
    skill_ids TEXT DEFAULT '[]',
    tool_endpoint_uuids TEXT DEFAULT '[]',
    orchestrate INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_by TEXT NOT NULL
);
