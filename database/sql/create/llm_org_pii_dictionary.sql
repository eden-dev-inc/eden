CREATE TABLE IF NOT EXISTS llm_org_pii_dictionary (
    organization_uuid UUID PRIMARY KEY REFERENCES organizations(uuid) ON DELETE CASCADE,
    terms JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
