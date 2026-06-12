CREATE TABLE IF NOT EXISTS llm_credentials (
    id UUID PRIMARY KEY,
    organization_uuid UUID NOT NULL REFERENCES organizations(uuid) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    label TEXT,
    description TEXT,
    base_url TEXT,
    api_key TEXT NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
