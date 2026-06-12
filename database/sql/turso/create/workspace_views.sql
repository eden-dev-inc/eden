CREATE TABLE IF NOT EXISTS workspace_views (
    organization_uuid TEXT NOT NULL,
    user_uuid TEXT NOT NULL,
    workspace_key TEXT NOT NULL DEFAULT 'dashboard-workspace',
    workspace_schema TEXT NOT NULL,
    saved_views TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (organization_uuid, user_uuid, workspace_key),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid) ON DELETE CASCADE,
    FOREIGN KEY (user_uuid) REFERENCES users(uuid) ON DELETE CASCADE
);
