CREATE TABLE IF NOT EXISTS agent_cards (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL UNIQUE REFERENCES llm_agents(id),
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    capabilities TEXT NOT NULL DEFAULT '[]',
    input_schema TEXT,
    output_schema TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
