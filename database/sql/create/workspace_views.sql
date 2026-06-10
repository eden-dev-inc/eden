CREATE TABLE IF NOT EXISTS workspace_views (
    organization_uuid UUID NOT NULL,
    user_uuid UUID NOT NULL,
    workspace_key TEXT NOT NULL DEFAULT 'dashboard-workspace',
    workspace_schema JSONB NOT NULL,
    saved_views JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_uuid, user_uuid, workspace_key),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid) ON DELETE CASCADE,
    FOREIGN KEY (user_uuid) REFERENCES users(uuid) ON DELETE CASCADE
);
