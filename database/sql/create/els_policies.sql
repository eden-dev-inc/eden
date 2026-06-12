CREATE TABLE IF NOT EXISTS els_policies (
    uuid UUID PRIMARY KEY,
    org_uuid UUID NOT NULL,
    endpoint_uuid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(endpoint_uuid, name)
);
CREATE INDEX IF NOT EXISTS idx_els_policies_org ON els_policies (org_uuid);
