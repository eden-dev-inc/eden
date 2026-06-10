CREATE TABLE IF NOT EXISTS llm_user_tools_endpoints (
    id UUID PRIMARY KEY,
    organization_uuid UUID NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    created_by UUID NOT NULL REFERENCES users(uuid),
    name TEXT NOT NULL,
    description TEXT,
    client_key TEXT NOT NULL,
    tools_url TEXT NOT NULL,
    bearer_token TEXT NOT NULL,
    tool_snapshot JSONB NOT NULL DEFAULT '[]'::jsonb,
    validated_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT llm_user_tools_endpoints_org_name_unique UNIQUE (organization_uuid, name),
    CONSTRAINT llm_user_tools_endpoints_client_key_unique UNIQUE (client_key)
);
