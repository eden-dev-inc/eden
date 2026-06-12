CREATE TABLE IF NOT EXISTS robots (
    uuid UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    organization_uuid UUID REFERENCES organizations(uuid) NOT NULL,
    api_key JSONB NOT NULL,
    description TEXT,
    ttl BIGINT,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_by UUID NOT NULL,
    updated_by UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (username, organization_uuid)
);
