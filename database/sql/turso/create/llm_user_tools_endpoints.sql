CREATE TABLE IF NOT EXISTS llm_user_tools_endpoints (
    id TEXT PRIMARY KEY,
    organization_uuid TEXT NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    created_by TEXT NOT NULL REFERENCES users(uuid),
    name TEXT NOT NULL,
    description TEXT,
    client_key TEXT NOT NULL,
    tools_url TEXT NOT NULL,
    bearer_token TEXT NOT NULL,
    tool_snapshot TEXT NOT NULL DEFAULT '[]',
    validated_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT llm_user_tools_endpoints_org_name_unique UNIQUE (organization_uuid, name),
    CONSTRAINT llm_user_tools_endpoints_client_key_unique UNIQUE (client_key)
);
